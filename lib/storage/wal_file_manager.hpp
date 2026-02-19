#ifndef WAL_FILE_MANAGER_H_INCLUDED
#define WAL_FILE_MANAGER_H_INCLUDED

#include <atomic>
#include <cstdint>
#include <map>
#include <vector>

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>

#include "memory_store.hpp"
#include "tsm_file_manager.hpp"

class Engine;

class WALFileManager {
private:
  int shardId;
  int currentWalSequenceNumber = -1;
  std::vector<seastar::shared_ptr<MemoryStore>> memoryStores;
  TSMFileManager *tsmFileManager;
  seastar::gate _backgroundGate;          // Tracks in-flight background TSM conversions
  seastar::semaphore compactionSemaphore{1}; // Only allow 1 rollover at a time
  seastar::semaphore _conversionSemaphore{1}; // Serialize background TSM conversions
  
public:
  WALFileManager();

  seastar::future<> init(Engine &engine, TSMFileManager &_tsmFileManager);
  template <class T>
  seastar::future<> insert(TSDBInsert<T> &insertRequest);
  template <class T>
  seastar::future<> insertBatch(std::vector<TSDBInsert<T>> &insertRequests);
  seastar::future<> rolloverMemoryStore();
  seastar::future<> convertWalToTsm(seastar::shared_ptr<MemoryStore> store);
  seastar::future<> close() {
    tsdb::wal_log.info("[WAL_CLOSE] Starting WAL file manager close on shard {}", shardId);

    // Drain all in-flight background TSM conversions before closing.
    // Guard against double-close (e.g., seastar::sharded<Engine> calling stop() twice).
    if (!_backgroundGate.is_closed()) {
      tsdb::wal_log.info("[WAL_CLOSE] Draining {} background TSM conversions on shard {}",
                         _backgroundGate.get_count(), shardId);
      co_await _backgroundGate.close();
      tsdb::wal_log.info("[WAL_CLOSE] Background TSM conversions drained on shard {}", shardId);
    }

    // Snapshot remaining stores. convertWalToTsm() erases from memoryStores
    // and calls removeWAL() internally, so we iterate a copy to avoid
    // iterator invalidation.
    auto snapshot = memoryStores;

    for (auto& store : snapshot) {
      if (!store) continue;

      if (!store->isEmpty()) {
        // Non-empty store: flush WAL to disk, then convert to TSM.
        try {
          tsdb::wal_log.info("[WAL_CLOSE] Flushing memory store {} to TSM on shard {}",
                             store->sequenceNumber, shardId);
          co_await store->close();          // flush WAL (idempotent)
          co_await convertWalToTsm(store);  // write TSM + erase from memoryStores + removeWAL
          tsdb::wal_log.info("[WAL_CLOSE] Successfully flushed store {} to TSM on shard {}",
                             store->sequenceNumber, shardId);
        } catch (const std::exception& e) {
          tsdb::wal_log.error("[WAL_CLOSE] Failed to flush store {} to TSM on shard {}: {} "
                              "(WAL preserved for recovery on next startup)",
                              store->sequenceNumber, shardId, e.what());
          // WAL file stays on disk — startup recovery will handle it.
        }
      } else {
        // Empty store: just close and remove the WAL file.
        try {
          tsdb::wal_log.info("[WAL_CLOSE] Closing empty memory store {} on shard {}",
                             store->sequenceNumber, shardId);
          co_await store->close();
          co_await store->removeWAL();
        } catch (const std::exception& e) {
          tsdb::wal_log.error("[WAL_CLOSE] Error closing empty store {} on shard {}: {}",
                              store->sequenceNumber, shardId, e.what());
        }
      }
    }

    memoryStores.clear();
    tsdb::wal_log.info("[WAL_CLOSE] WAL file manager closed on shard {}", shardId);
  }
  std::optional<TSMValueType> getSeriesType(const std::string &seriesKey);
  std::optional<TSMValueType> getSeriesType(const SeriesId128 &seriesId);
  
  // Query memory stores for data (deletion filtering removed - WAL replay handles current state)
  // Returns a const pointer to the in-memory series, or nullptr if not found.
  // NOTE: With background TSM conversion, multiple memory stores may contain
  // data for the same series. This returns only the first match. Use
  // queryAllMemoryStores() to get data from all stores.
  template <class T>
  const InMemorySeries<T>* queryMemoryStores(const std::string& seriesKey) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
    return queryMemoryStores<T>(seriesId);
  }

  template <class T>
  const InMemorySeries<T>* queryMemoryStores(const SeriesId128& seriesId) {
    for (auto& memStore : memoryStores) {
      auto result = memStore->querySeries<T>(seriesId);
      if (result != nullptr) {
        return result;
      }
    }
    return nullptr;
  }

  // Query ALL memory stores and return pointers to every matching series.
  // Needed because background TSM conversion means multiple stores may
  // hold data for the same series simultaneously.
  template <class T>
  std::vector<const InMemorySeries<T>*> queryAllMemoryStores(const std::string& seriesKey) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
    return queryAllMemoryStores<T>(seriesId);
  }

  template <class T>
  std::vector<const InMemorySeries<T>*> queryAllMemoryStores(const SeriesId128& seriesId) {
    std::vector<const InMemorySeries<T>*> results;
    for (auto& memStore : memoryStores) {
      auto result = memStore->querySeries<T>(seriesId);
      if (result != nullptr) {
        results.push_back(result);
      }
    }
    return results;
  }
  
  
  // Check if ANY memory store has data for this series within [startTime, endTime].
  // Returns on first match without allocating a vector.  Used by pushdown
  // aggregation to decide whether to fall back to the full query path.
  template <class T>
  bool hasMemoryDataInRange(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime) {
    for (auto& memStore : memoryStores) {
      auto* series = memStore->template querySeries<T>(seriesId);
      if (series && !series->timestamps.empty()) {
        auto it = std::lower_bound(
            series->timestamps.begin(), series->timestamps.end(), startTime);
        if (it != series->timestamps.end() && *it <= endTime) {
          return true;
        }
      }
    }
    return false;
  }

  // Return the earliest timestamp across all memory stores for a given series
  // within [startTime, endTime].  Returns nullopt if no memory data in range.
  // Used by pushdown aggregation to determine the split point between the
  // TSM-only portion (full pushdown) and the overlap portion (fallback).
  template <class T>
  std::optional<uint64_t> getEarliestMemoryTimestamp(const SeriesId128& seriesId,
                                                      uint64_t startTime, uint64_t endTime) {
    std::optional<uint64_t> earliest;
    for (auto& memStore : memoryStores) {
      auto* series = memStore->template querySeries<T>(seriesId);
      if (series && !series->timestamps.empty()) {
        auto it = std::lower_bound(
            series->timestamps.begin(), series->timestamps.end(), startTime);
        if (it != series->timestamps.end() && *it <= endTime) {
          uint64_t ts = *it;
          if (!earliest.has_value() || ts < *earliest) {
            earliest = ts;
          }
        }
      }
    }
    return earliest;
  }

  // Delete data from memory stores and write to WAL
  seastar::future<> deleteFromMemoryStores(const std::string& seriesKey,
                                          uint64_t startTime,
                                          uint64_t endTime);
};

#endif