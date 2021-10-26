#ifndef __ENGINE_H_INCLUDED__
#define __ENGINE_H_INCLUDED__

#include <map>
#include <vector>
#include <memory>

#include "tsdb_value.hpp"
#include "memory_store.hpp"
#include "tsm_file_manager.hpp"
#include "query_result.hpp"

class Engine {
private:
  TSMFileManager fileManager;

  // TODO: Should Memstores have its own 'manager' for wal naming?
  int currentWalSequenceNumber = -1;
  std::shared_ptr<MemoryStore> memoryStore;
  std::vector<std::shared_ptr<MemoryStore>> pendingMemoryStores;

public:
  Engine();
  template <class T>
  void insert(TSDBInsert<T> insertRequest);
  void rolloverMemoryStore();
  VariantQueryResult query(std::string series, uint64_t startTime, uint64_t endTime);
};



#endif
