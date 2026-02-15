#ifndef WAL_FILE_MANAGER_H_INCLUDED
#define WAL_FILE_MANAGER_H_INCLUDED

#include <atomic>
#include <cstdint>
#include <map>
#include <vector>

#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/pipe.hh>

#include "memory_store.hpp"
#include "tsm_file_manager.hpp"

class Engine;

class WALFileManager {
private:
  int shardId;
  int currentWalSequenceNumber = -1;
  std::vector<seastar::shared_ptr<MemoryStore>> memoryStores;
  TSMFileManager *tsmFileManager;
  seastar::pipe<seastar::shared_ptr<MemoryStore>> pendingWrites;
  seastar::semaphore compactionSemaphore{1}; // Only allow 1 compaction at a time
  
public:
  WALFileManager();

  seastar::future<> init(Engine &engine, TSMFileManager &_tsmFileManager);
  template <class T>
  seastar::future<> insert(TSDBInsert<T> &insertRequest);
  template <class T>
  seastar::future<> insertBatch(std::vector<TSDBInsert<T>> &insertRequests);
  seastar::future<> rolloverMemoryStore();
  seastar::future<> convertWalToTsm(seastar::shared_ptr<MemoryStore> store);
  seastar::future<> startTsmWriter();
  seastar::future<> close() {
    tsdb::wal_log.info("[WAL_CLOSE] Starting WAL file manager close on shard {}", shardId);

    try {
      // Ensure current memory store is properly flushed
      if (!memoryStores.empty() && memoryStores[0]) {
        tsdb::wal_log.info("[WAL_CLOSE] Closing current memory store {} on shard {}",
                           memoryStores[0]->sequenceNumber, shardId);
        co_await memoryStores[0]->close();
      }
      tsdb::wal_log.info("[WAL_CLOSE] WAL file manager closed on shard {}", shardId);
    } catch (const std::exception& e) {
      tsdb::wal_log.error("[WAL_CLOSE] Error during WAL close on shard {}: {}", shardId, e.what());
      // Don't rethrow during shutdown to avoid hanging
    }
  }
  std::optional<TSMValueType> getSeriesType(std::string &seriesKey);
  
  // Query memory stores for data (deletion filtering removed - WAL replay handles current state)
  // Returns a const pointer to the in-memory series, or nullptr if not found.
  template <class T>
  const InMemorySeries<T>* queryMemoryStores(const std::string& seriesKey) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
    for (auto& memStore : memoryStores) {
      // Direct query - no filtering needed since WAL replay maintains correct state
      auto result = memStore->querySeries<T>(seriesId);
      if (result != nullptr) {
        return result;
      }
    }
    return nullptr;
  }
  
  
  // Delete data from memory stores and write to WAL
  seastar::future<> deleteFromMemoryStores(const std::string& seriesKey, 
                                          uint64_t startTime,
                                          uint64_t endTime);
};

#endif