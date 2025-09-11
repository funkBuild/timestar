#ifndef __TSM_H_INCLUDED__
#define __TSM_H_INCLUDED__

#include "query_result.hpp"
#include "tsm_result.hpp"
#include "tsm_tombstone.hpp"
#include "series_id.hpp"

#include <string>
#include <memory>
#include <fstream>
#include <unordered_map>
#include <vector>
#include <tuple>
#include <optional>

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>

enum class TSMValueType { Float = 0, Boolean, String };

typedef struct TSMIndexBlock {
  uint64_t minTime;
  uint64_t maxTime;
  uint64_t offset;
  uint32_t size;
} TSMIndexBlock;

typedef struct TSMIndexEntry {
  SeriesId128 seriesId;
  TSMValueType seriesType;
  std::vector<TSMIndexBlock> indexBlocks;

  bool operator < (const TSMIndexEntry& str) const
  {
      return (seriesId > str.seriesId);
  }
} TSMIndexEntry;

class TSM {
private:
  std::string filePath;
  seastar::file tsmFile;
  uint64_t length = 0;

  // TODO: Test using tsl::htrie_map to save memory
  std::unordered_map<SeriesId128, TSMIndexEntry> index;
  
  // Tombstone support
  std::unique_ptr<tsdb::TSMTombstone> tombstones;

  // Reference counting for safe deletion during compaction
  std::atomic<int32_t> refCount{0};
  std::atomic<bool> markedForDeletion{false};
  
  // Helper to get tombstone file path
  std::string getTombstonePath() const;

public:
  uint64_t tierNum;
  uint64_t seqNum;

  TSM(std::string _absoluteFilePath);
  seastar::future<> open();
  seastar::future<> close();
  uint64_t rankAsInteger();
  
  // Reference counting methods for non-blocking reads during compaction
  void addRef() { refCount.fetch_add(1, std::memory_order_relaxed); }
  void releaseRef() { 
    if (refCount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      if (markedForDeletion.load(std::memory_order_acquire)) {
        // Last reference released and file is marked for deletion
        // Note: We can't directly call async function here
        // In production, this would trigger async deletion via executor
        scheduleDeletionFlag = true;
      }
    }
  }
  int32_t getRefCount() const { return refCount.load(std::memory_order_relaxed); }
  
  // Mark file for deletion after compaction
  void markForDeletion() { 
    markedForDeletion.store(true, std::memory_order_release);
    if (getRefCount() == 0) {
      scheduleDeletionFlag = true;
    }
  }
  
  bool scheduleDeletionFlag = false;
  
  // Schedule async deletion
  seastar::future<> scheduleDelete();

  seastar::future<> readIndex();
  template <class T>
  seastar::future<> readSeries(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime, TSMResult<T> &results);
  template <class T>
  seastar::future<> readBlock(const TSMIndexBlock &indexBlock, uint64_t startTime, uint64_t endTime, TSMResult<T> &results);
  std::optional<TSMValueType> getSeriesType(const SeriesId128& seriesId);
  
  // Get all series IDs in this file (for compaction)
  std::vector<SeriesId128> getSeriesIds() const {
    std::vector<SeriesId128> ids;
    ids.reserve(index.size());
    for (const auto& [id, entry] : index) {
      ids.push_back(id);
    }
    return ids;
  }
  
  // Get file size for compaction planning
  uint64_t getFileSize() const { return length; }

  template <class T>
  static constexpr TSMValueType getValueType(){
    if(std::is_same<T, double>::value){
      return TSMValueType::Float;
    } else if(std::is_same<T, bool>::value){
      return TSMValueType::Boolean;
    } else if(std::is_same<T, std::string>::value){
      return TSMValueType::String;
    }
  };
  
  // Tombstone support methods
  seastar::future<> loadTombstones();
  
  // Delete range with verification
  seastar::future<bool> deleteRange(
    const SeriesId128& seriesId,
    uint64_t startTime,
    uint64_t endTime
  );
  
  // Check if series exists in time range (for verification)
  bool hasSeriesInTimeRange(
    const SeriesId128& seriesId,
    uint64_t startTime,
    uint64_t endTime
  ) const;
  
  // Check if TSM file could contain data in time range (more permissive, for deletes)
  bool couldContainTimeRange(
    uint64_t startTime,
    uint64_t endTime
  ) const;
  
  // Query with tombstone filtering
  template <class T>
  seastar::future<TSMResult<T>> queryWithTombstones(
    const SeriesId128& seriesId,
    uint64_t startTime,
    uint64_t endTime
  );
  
  // Get tombstone manager (for compaction)
  tsdb::TSMTombstone* getTombstones() { return tombstones.get(); }
  bool hasTombstones() const { return tombstones && tombstones->getEntryCount() > 0; }
  
  // Delete tombstone file after compaction
  seastar::future<> deleteTombstoneFile();
  
  // Get series ID hash for tombstone compatibility
  uint64_t getSeriesIdHash(const SeriesId128& seriesId) const;
};

#endif
