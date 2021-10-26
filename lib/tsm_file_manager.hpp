#ifndef __TSM_FILE_MANAGER_H_INCLUDED__
#define __TSM_FILE_MANAGER_H_INCLUDED__

#include <vector>
#include <cstdint>
#include <atomic>
#include <map>

#include "aligned_buffer.hpp"
#include "tsm.hpp"
#include "memory_store.hpp"


class TSMFileManager {
private:
  //std::vector<std::shared_ptr<TSM>> tiers[4];
  std::atomic<unsigned int> sequenceId = 0;
  std::vector<std::shared_ptr<TSM>> tsmFiles;

  void init();
  void openTsmFile(std::string path);
public:
  std::map<unsigned int, std::shared_ptr<TSM>> sequencedTsmFiles;

  TSMFileManager();

  void queueMemstoreCompaction(std::shared_ptr<MemoryStore> memStore);
  void writeMemstore(std::shared_ptr<MemoryStore> memStore);

};

#endif