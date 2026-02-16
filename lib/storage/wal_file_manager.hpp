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

    for (auto& store : memoryStores) {
      if (!store) continue;
      try {
        tsdb::wal_log.info("[WAL_CLOSE] Closing memory store {} on shard {}",
                           store->sequenceNumber, shardId);
        co_await store->close();
      } catch (const std::exception& e) {
        tsdb::wal_log.error("[WAL_CLOSE] Error closing memory store {} on shard {}: {}",
                           store->sequenceNumber, shardId, e.what());
        // Continue closing remaining stores
      }
    }
    tsdb::wal_log.info("[WAL_CLOSE] WAL file manager closed on shard {}", shardId);
  }
  std::optional<TSMValueType> getSeriesType(const std::string &seriesKey);
  
  // Query memory stores for data (deletion filtering removed - WAL replay handles current state)
  // Returns a const pointer to the in-memory series, or nullptr if not found.
  // NOTE: With background TSM conversion, multiple memory stores may contain
  // data for the same series. This returns only the first match. Use
  // queryAllMemoryStores() to get data from all stores.
  template <class T>
  const InMemorySeries<T>* queryMemoryStores(const std::string& seriesKey) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
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
    std::vector<const InMemorySeries<T>*> results;
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
    for (auto& memStore : memoryStores) {
      auto result = memStore->querySeries<T>(seriesId);
      if (result != nullptr) {
        results.push_back(result);
      }
    }
    return results;
  }
  
  
  // Delete data from memory stores and write to WAL
  seastar::future<> deleteFromMemoryStores(const std::string& seriesKey, 
                                          uint64_t startTime,
                                          uint64_t endTime);
};

#endif