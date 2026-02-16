#ifndef TSM_FILE_MANAGER_H_INCLUDED
#define TSM_FILE_MANAGER_H_INCLUDED

#include <vector>
#include <cstdint>
#include <map>
#include <optional>

#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>

#include "aligned_buffer.hpp"
#include "tsm.hpp"
#include "memory_store.hpp"

// Forward declaration
class TSMCompactor;

class TSMFileManager {
private:
  static constexpr size_t MAX_TIERS = 5;
  static constexpr size_t FILES_PER_COMPACTION = 4;
  
  int shardId;
  // No atomic needed: TSMFileManager is a per-shard object in Seastar's shard-per-core model,
  // only accessed from a single thread.
  unsigned int nextSequenceId = 0;
  std::vector<seastar::shared_ptr<TSM>> tsmFiles;
  
  // Track files by tier for compaction
  std::vector<seastar::shared_ptr<TSM>> tiers[MAX_TIERS];
  
  // Compactor (using shared_ptr to work with forward declaration)
  std::shared_ptr<TSMCompactor> compactor;
  std::optional<seastar::future<>> compactionTask;

  seastar::future<> openTsmFile(std::string path);
  std::string basePath();
  seastar::future<> checkAndTriggerCompaction();
  
public:
  std::map<unsigned int, seastar::shared_ptr<TSM>> sequencedTsmFiles;

  TSMFileManager();

  seastar::future<> init();
  seastar::future<> writeMemstore(seastar::shared_ptr<MemoryStore> memStore, uint64_t tier = 0);
  std::optional<TSMValueType> getSeriesType(const std::string &seriesKey);
  
  // Compaction support
  std::vector<seastar::shared_ptr<TSM>> getFilesInTier(uint64_t tier) const;
  size_t getFileCountInTier(uint64_t tier) const;
  bool shouldCompactTier(uint64_t tier) const;
  seastar::future<> addTSMFile(seastar::shared_ptr<TSM> file);
  seastar::future<> removeTSMFiles(const std::vector<seastar::shared_ptr<TSM>>& files);
  
  // Start background compaction
  seastar::future<> startCompactionLoop();
  seastar::future<> stopCompactionLoop();
};

#endif