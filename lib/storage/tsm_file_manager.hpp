#ifndef TSM_FILE_MANAGER_H_INCLUDED
#define TSM_FILE_MANAGER_H_INCLUDED

#include <vector>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>

#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>

#include "aligned_buffer.hpp"
#include "tsm.hpp"
#include "memory_store.hpp"
#include "tsdb_config.hpp"

// Forward declaration
class TSMCompactor;

class TSMFileManager {
private:
  static constexpr size_t MAX_TIERS = 5;
  static size_t filesPerCompaction() { return tsdb::config().storage.compaction.tier0_min_files; }
  
  int shardId;
  // No atomic needed: TSMFileManager is a per-shard object in Seastar's shard-per-core model,
  // only accessed from a single thread.
  unsigned int nextSequenceId = 0;
  std::vector<seastar::shared_ptr<TSM>> tsmFiles;
  
  // Track files by tier for compaction
  std::vector<seastar::shared_ptr<TSM>> tiers[MAX_TIERS];
  
  // Compactor (unique ownership, destructor defined in .cpp where TSMCompactor is complete)
  std::unique_ptr<TSMCompactor> compactor;
  std::optional<seastar::future<>> compactionTask;

  seastar::future<> openTsmFile(std::string path);
  std::string basePath();
  seastar::future<> checkAndTriggerCompaction();
  
public:
  std::map<unsigned int, seastar::shared_ptr<TSM>> sequencedTsmFiles;

  TSMFileManager();
  ~TSMFileManager();  // Defined in .cpp where TSMCompactor is complete

  seastar::future<> init();
  seastar::future<> writeMemstore(seastar::shared_ptr<MemoryStore> memStore, uint64_t tier = 0);
  std::optional<TSMValueType> getSeriesType(const std::string &seriesKey);
  std::optional<TSMValueType> getSeriesType(const SeriesId128 &seriesId);
  
  // Compaction support
  std::vector<seastar::shared_ptr<TSM>> getFilesInTier(uint64_t tier) const;
  size_t getFileCountInTier(uint64_t tier) const;
  bool shouldCompactTier(uint64_t tier) const;
  seastar::future<> addTSMFile(seastar::shared_ptr<TSM> file);
  seastar::future<> removeTSMFiles(const std::vector<seastar::shared_ptr<TSM>>& files);
  
  // Allocate a globally unique sequence ID for new TSM files
  unsigned int allocateSequenceId() { return nextSequenceId++; }

  // Get the compactor (for tombstone rewrites)
  TSMCompactor* getCompactor() { return compactor.get(); }

  // Start background compaction
  seastar::future<> startCompactionLoop();
  seastar::future<> stopCompactionLoop();
};

#endif