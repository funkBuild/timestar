#ifndef __WAL_FILE_MANAGER_H_INCLUDED__
#define __WAL_FILE_MANAGER_H_INCLUDED__

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
  seastar::future<> rolloverMemoryStore();
  seastar::future<> convertWalToTsm(seastar::shared_ptr<MemoryStore> store);
  seastar::future<> startTsmWriter();
  seastar::future<> close() {
    // Ensure current memory store is properly flushed
    if (!memoryStores.empty() && memoryStores[0]) {
      co_await memoryStores[0]->close();
    }
    seastar::shared_ptr<MemoryStore> eofPtr = nullptr;
    co_await pendingWrites.writer.write(std::move(eofPtr));
  }
  std::optional<TSMValueType> getSeriesType(std::string &seriesKey);
  
  // Query memory stores for data
  template <class T>
  std::optional<InMemorySeries<T>> queryMemoryStores(const std::string& seriesKey) {
    for (auto& memStore : memoryStores) {
      auto result = memStore->querySeries<T>(seriesKey);
      if (result.has_value()) {
        return result;
      }
    }
    return std::nullopt;
  }
  
  // Delete data from memory stores and write to WAL
  seastar::future<> deleteFromMemoryStores(const std::string& seriesKey, 
                                          uint64_t startTime,
                                          uint64_t endTime);
};

#endif