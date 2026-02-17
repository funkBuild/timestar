#include "memory_store.hpp"
#include "util.hpp"
#include "logger.hpp"
#include "logging_config.hpp"

#include <stdexcept>
#include <chrono>
#include <algorithm>
#include <numeric>

template <class T>
void InMemorySeries<T>::insert(TSDBInsert<T>&& insertRequest){
  if (insertRequest.getTimestamps().empty()) return;

  size_t oldSize = timestamps.size();

  if (oldSize == 0) {
    // Series is empty -- take ownership of the entire vectors via move.
    // takeTimestamps() materializes shared timestamps into owned if needed.
    timestamps = insertRequest.takeTimestamps();
    values = std::move(insertRequest.values);
  } else {
    // Append new data. Use getTimestamps() for read access (works with shared).
    const auto& srcTimestamps = insertRequest.getTimestamps();
    timestamps.insert(timestamps.end(), srcTimestamps.begin(), srcTimestamps.end());
    values.insert(values.end(),
                  std::make_move_iterator(insertRequest.values.begin()),
                  std::make_move_iterator(insertRequest.values.end()));
  }

  if (oldSize == 0) {
    // First insert -- check if already sorted
    bool sorted = true;
    for (size_t i = 1; i < timestamps.size(); ++i) {
      if (timestamps[i] < timestamps[i - 1]) {
        sorted = false;
        break;
      }
    }
    if (!sorted) {
      sort(0);
    }
    return;
  }

  // Check if new data starts at or after the last existing timestamp
  if (timestamps[oldSize] >= timestamps[oldSize - 1]) {
    // Boundary is fine -- check if new data itself is sorted
    bool newDataSorted = true;
    for (size_t i = oldSize + 1; i < timestamps.size(); ++i) {
      if (timestamps[i] < timestamps[i - 1]) {
        newDataSorted = false;
        break;
      }
    }
    if (newDataSorted) return;  // Everything already sorted
  }

  // Data is not sorted -- merge-sort only the new suffix, then merge with existing
  sort(oldSize);
}

template <class T>
void InMemorySeries<T>::sort(size_t sortedPrefix){
  size_t n = timestamps.size();
  if (n <= 1) return;

  // Build index array
  std::vector<size_t> indices(n);
  std::iota(indices.begin(), indices.end(), 0);

  if (sortedPrefix > 0 && sortedPrefix < n) {
    // The prefix [0, sortedPrefix) is already sorted.
    // Sort only the suffix indices [sortedPrefix, n) by timestamp.
    std::sort(indices.begin() + sortedPrefix, indices.end(),
              [this](size_t a, size_t b) { return timestamps[a] < timestamps[b]; });

    // Merge the two sorted index ranges into one sorted index sequence.
    std::inplace_merge(indices.begin(), indices.begin() + sortedPrefix, indices.end(),
                       [this](size_t a, size_t b) { return timestamps[a] < timestamps[b]; });
  } else {
    // No sorted prefix -- full sort (first insert with unsorted data)
    std::sort(indices.begin(), indices.end(),
              [this](size_t a, size_t b) { return timestamps[a] < timestamps[b]; });
  }

  // Apply permutation via temporary vectors for sequential access patterns and
  // better cache locality. The O(N) extra memory is cheap on Seastar's
  // thread-local allocator (same-shard allocation) and avoids the indirect
  // index jumps of a cycle-chase approach.
  std::vector<uint64_t> sortedTs(n);
  std::vector<T> sortedVals(n);
  for (size_t i = 0; i < n; i++) {
    sortedTs[i] = timestamps[indices[i]];
    sortedVals[i] = std::move(values[indices[i]]);
  }
  timestamps = std::move(sortedTs);
  values = std::move(sortedVals);
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

  // Use the larger of actual WAL size and cumulative estimated size.
  // Compression can make the actual WAL size much smaller than estimates,
  // so relying only on actual size may never trigger rollover.
  size_t walSize = wal->getCurrentSize();
  size_t effectiveSize = std::max(walSize, estimatedAccumulatedSize);
  co_return effectiveSize >= WAL_SIZE_THRESHOLD;
}

template <class T>
void MemoryStore::insertMemory(TSDBInsert<T>&& insertRequest){
  // In-memory insert
  SeriesId128 seriesId = insertRequest.seriesId128();

  // robin_map's operator[] returns a mutable reference, creating entry if needed.
  // Default-constructed variant will be InMemorySeries<double> (first alternative).
  auto& variantSeries = series[seriesId];

  // Single variant access: std::get_if returns a pointer if the type matches, nullptr otherwise.
  // This replaces the previous 2-3 separate variant accesses
  // (std::visit for empty check + std::holds_alternative for type check + std::get for access).
  auto* typedSeries = std::get_if<InMemorySeries<T>>(&variantSeries);

  if (!typedSeries || typedSeries->timestamps.empty()) {
    // Either wrong type (e.g. default-constructed double when T != double) or empty.
    // Before overwriting, check whether the current variant has existing data of a different type.
    if (typedSeries == nullptr) {
      bool hasData = std::visit([](const auto& s) { return !s.timestamps.empty(); }, variantSeries);
      if (hasData) {
        throw std::runtime_error("Type mismatch: series " + insertRequest.seriesKey() + " already exists with a different type");
      }
    }
    variantSeries = InMemorySeries<T>();
    typedSeries = &std::get<InMemorySeries<T>>(variantSeries);
  }

  typedSeries->insert(std::move(insertRequest));
}

template <class T>
bool MemoryStore::wouldExceedThreshold(TSDBInsert<T> &insertRequest){
  if(!wal) {
    return false;
  }

  size_t estimatedSize = wal->estimateInsertSize(insertRequest);
  // Use the larger of actual WAL size and cumulative estimated size for
  // the base, ensuring rollover triggers even when compression is highly
  // effective and actual WAL size grows slowly.
  size_t effectiveSize = std::max(wal->getCurrentSize(), estimatedAccumulatedSize);

  return (effectiveSize + estimatedSize) >= WAL_SIZE_THRESHOLD;
}

template <class T>
bool MemoryStore::wouldBatchExceedThreshold(std::vector<TSDBInsert<T>> &insertRequests){
  if(!wal || insertRequests.empty()) {
    return false;
  }

  size_t totalEstimatedSize = 0;
  for(auto& insertRequest : insertRequests) {
    totalEstimatedSize += wal->estimateInsertSize(insertRequest);
  }

  // Use the larger of actual WAL size and cumulative estimated size
  size_t effectiveSize = std::max(wal->getCurrentSize(), estimatedAccumulatedSize);

  return (effectiveSize + totalEstimatedSize) >= WAL_SIZE_THRESHOLD;
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

  // Compute estimated size before the WAL write (used for rollover tracking)
  size_t thisEstimatedSize = 0;
  if(wal) {
    thisEstimatedSize = wal->estimateInsertSize(insertRequest);
  }

  // Try to insert to WAL
  if(wal) {
    auto result = co_await wal->insert(insertRequest);
    if (result == WALInsertResult::RolloverNeeded) {
      tsdb::memory_log.debug("WAL signaled rollover needed during insert");
      co_return true;  // Signal rollover needed
    }
    LOG_INSERT_PATH(tsdb::memory_log, trace, "WAL insert completed for series: {}", insertRequest.seriesKey());
  }

  // Track cumulative estimated size for rollover decisions
  estimatedAccumulatedSize += thisEstimatedSize;

  // Only insert to memory if WAL write succeeded -- move data since WAL
  // has already persisted it and the caller won't use it again.
  insertMemory(std::move(insertRequest));

  co_return false; // No rollover needed
}

template <class T>
seastar::future<bool> MemoryStore::insertBatch(std::vector<TSDBInsert<T>> &insertRequests, size_t preComputedBatchSize){
  if(closed)
    throw std::runtime_error("MemoryStore is closed");

  if(insertRequests.empty()) {
    co_return false; // No work to do
  }

#if TSDB_LOG_INSERT_PATH
  auto start_memory_batch = std::chrono::high_resolution_clock::now();
#endif
  LOG_INSERT_PATH(tsdb::memory_log, info, "[PERF] [MEMORY] Batch insert started for {} requests", insertRequests.size());

  // Compute the estimated batch size if not pre-computed
  size_t batchEstimate = preComputedBatchSize;
  if(batchEstimate == 0 && wal) {
    for(auto& insertRequest : insertRequests) {
      batchEstimate += wal->estimateInsertSize(insertRequest);
    }
  }

  // Check if this batch would exceed the WAL threshold.
  // Use the larger of actual WAL size and cumulative estimated size as the
  // base. Compression can make the actual WAL size much smaller than the
  // uncompressed estimate, so relying only on actual compressed size may
  // never trigger rollover for highly compressible data.
  bool needsRollover = false;
  if(wal) {
    size_t effectiveSize = std::max(wal->getCurrentSize(), estimatedAccumulatedSize);
    needsRollover = (effectiveSize + batchEstimate) >= WAL_SIZE_THRESHOLD;
  }
  if(needsRollover) {
    // Don't insert - signal that rollover is needed
    LOG_INSERT_PATH(tsdb::memory_log, debug, "Batch insert would exceed WAL limit, signaling rollover needed");
    co_return true;
  }

  // Try to insert batch to WAL using the existing batch functionality
#if TSDB_LOG_INSERT_PATH
  auto start_wal_insert = std::chrono::high_resolution_clock::now();
#endif
  if(wal) {
    auto result = co_await wal->insertBatch(insertRequests);
    if (result == WALInsertResult::RolloverNeeded) {
      LOG_INSERT_PATH(tsdb::memory_log, debug, "WAL signaled rollover needed during batch insert");
      co_return true;  // Signal rollover needed
    }
    LOG_INSERT_PATH(tsdb::memory_log, trace, "WAL batch insert completed for {} requests", insertRequests.size());
  }
#if TSDB_LOG_INSERT_PATH
  auto end_wal_insert = std::chrono::high_resolution_clock::now();
#endif

  // Track cumulative estimated size for rollover decisions
  estimatedAccumulatedSize += batchEstimate;

  // Only insert to memory if WAL write succeeded - move data since WAL
  // has already persisted it and individual elements won't be reused.
#if TSDB_LOG_INSERT_PATH
  auto start_memory_insert = std::chrono::high_resolution_clock::now();
#endif
  for(auto& insertRequest : insertRequests) {
    insertMemory(std::move(insertRequest));
  }
#if TSDB_LOG_INSERT_PATH
  auto end_memory_insert = std::chrono::high_resolution_clock::now();

  auto end_memory_batch = std::chrono::high_resolution_clock::now();
  auto wal_insert_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_wal_insert - start_wal_insert);
  auto memory_insert_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_memory_insert - start_memory_insert);
  auto total_memory_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_memory_batch - start_memory_batch);
#endif

#if TSDB_LOG_INSERT_PATH
  LOG_INSERT_PATH(tsdb::memory_log, info, "[PERF] [MEMORY] WAL batch insert: {}μs", wal_insert_duration.count());
  LOG_INSERT_PATH(tsdb::memory_log, info, "[PERF] [MEMORY] In-memory batch insert: {}μs", memory_insert_duration.count());
  LOG_INSERT_PATH(tsdb::memory_log, info, "[PERF] [MEMORY] Total batch insert: {}μs", total_memory_duration.count());
#endif

  co_return false; // No rollover needed
}

std::optional<TSMValueType> MemoryStore::getSeriesType(const SeriesId128 &seriesId){
  auto it = series.find(seriesId);

  if(it == series.end())
    return {};
  
  return (TSMValueType)it->second.index();
}


template void InMemorySeries<double>::insert(TSDBInsert<double>&& insertRequest);
template void InMemorySeries<double>::sort(size_t);
template void InMemorySeries<bool>::insert(TSDBInsert<bool>&& insertRequest);
template void InMemorySeries<bool>::sort(size_t);
template void InMemorySeries<std::string>::insert(TSDBInsert<std::string>&& insertRequest);
template void InMemorySeries<std::string>::sort(size_t);

template seastar::future<bool> MemoryStore::insert<double>(TSDBInsert<double> &insertRequest);
template seastar::future<bool> MemoryStore::insert<bool>(TSDBInsert<bool> &insertRequest);
template seastar::future<bool> MemoryStore::insert<std::string>(TSDBInsert<std::string> &insertRequest);
template seastar::future<bool> MemoryStore::insertBatch<double>(std::vector<TSDBInsert<double>> &insertRequests, size_t preComputedBatchSize);
template seastar::future<bool> MemoryStore::insertBatch<bool>(std::vector<TSDBInsert<bool>> &insertRequests, size_t preComputedBatchSize);
template seastar::future<bool> MemoryStore::insertBatch<std::string>(std::vector<TSDBInsert<std::string>> &insertRequests, size_t preComputedBatchSize);
template void MemoryStore::insertMemory<double>(TSDBInsert<double>&& insertRequest);
template void MemoryStore::insertMemory<bool>(TSDBInsert<bool>&& insertRequest);
template void MemoryStore::insertMemory<std::string>(TSDBInsert<std::string>&& insertRequest);
template bool MemoryStore::wouldExceedThreshold<double>(TSDBInsert<double> &insertRequest);
template bool MemoryStore::wouldExceedThreshold<bool>(TSDBInsert<bool> &insertRequest);
template bool MemoryStore::wouldExceedThreshold<std::string>(TSDBInsert<std::string> &insertRequest);
template bool MemoryStore::wouldBatchExceedThreshold<double>(std::vector<TSDBInsert<double>> &insertRequests);
template bool MemoryStore::wouldBatchExceedThreshold<bool>(std::vector<TSDBInsert<bool>> &insertRequests);
template bool MemoryStore::wouldBatchExceedThreshold<std::string>(std::vector<TSDBInsert<std::string>> &insertRequests);

void MemoryStore::deleteRange(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime) {
    tsdb::memory_log.debug("Deleting range for series {} from {} to {}",
                          seriesId.toHex(), startTime, endTime);

    // Find the series in memory - use const find first to check existence
    auto cit = series.find(seriesId);
    if (cit == series.end()) {
        return; // No data to delete
    }

    // Access mutable reference through the map using at()
    auto& variantSeries = series.at(seriesId);

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

    }, variantSeries);

    // If all data was removed, remove the series entirely
    bool isEmpty = std::visit([](const auto& inMemorySeries) {
        return inMemorySeries.timestamps.empty();
    }, variantSeries);

    if (isEmpty) {
        series.erase(seriesId);
    }
}