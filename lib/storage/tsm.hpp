#ifndef TSM_H_INCLUDED
#define TSM_H_INCLUDED

#include "query_result.hpp"
#include "tsm_result.hpp"
#include "tsm_tombstone.hpp"
#include "series_id.hpp"
#include "bloom_filter.hpp"
#include "block_aggregator.hpp"

#include <string>
#include <memory>
#include <fstream>
#include <vector>
#include <list>
#include <tuple>
#include <optional>
#include <unordered_map>
#include <tsl/robin_map.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>

// Forward declarations
class Slice;

enum class TSMValueType { Float = 0, Boolean, String };

typedef struct TSMIndexBlock {
  uint64_t minTime;
  uint64_t maxTime;
  uint64_t offset;
  uint32_t size;
} TSMIndexBlock;

// Batch of contiguous blocks for optimized I/O
struct BlockBatch {
  uint64_t startOffset;           // Offset of first block in batch
  uint64_t totalSize;             // Sum of all block sizes (uint64_t to avoid overflow)
  std::vector<TSMIndexBlock> blocks;  // Blocks in this batch

  BlockBatch() : startOffset(0), totalSize(0) {}
};

// Sparse index entry for lazy loading
struct SparseIndexEntry {
  SeriesId128 seriesId;    // 16 bytes - for hash map key
  uint64_t fileOffset;     // 8 bytes - where to read in file
  uint32_t entrySize;      // 4 bytes - how much to read
  TSMValueType seriesType; // series value type (captured during sparse index parse)
};

typedef struct TSMIndexEntry {
  SeriesId128 seriesId;
  TSMValueType seriesType;
  std::vector<TSMIndexBlock> indexBlocks;
} TSMIndexEntry;

class TSM {
private:
  std::string filePath;
  seastar::file tsmFile;
  uint64_t length = 0;

  // Lazy loading: sparse index + bloom filter for memory efficiency
  tsl::robin_map<SeriesId128, SparseIndexEntry, SeriesId128::Hash> sparseIndex;
  bloom_filter seriesBloomFilter;

  // Full index cache with LRU eviction for hot series
  // LRU list: front = most recently used, back = least recently used
  using LRUList = std::list<std::pair<SeriesId128, TSMIndexEntry>>;
  mutable LRUList lruList;
  mutable std::unordered_map<SeriesId128, LRUList::iterator, SeriesId128::Hash> fullIndexCache;

  // Configuration for bloom filter and cache
  static constexpr double BLOOM_FPR = 0.001;  // 0.1% false positive rate
  static constexpr size_t MAX_CACHE_ENTRIES = 4096;

  // Tombstone support
  std::unique_ptr<tsdb::TSMTombstone> tombstones;

  // Helper to get tombstone file path
  std::string getTombstonePath() const;

public:
  uint64_t tierNum;
  uint64_t seqNum;

  TSM(std::string _absoluteFilePath);
  seastar::future<> open();
  seastar::future<> close();
  uint64_t rankAsInteger();
  
  // Schedule async deletion — closes file and removes from disk
  seastar::future<> scheduleDelete();

  // Lazy loading index methods
  seastar::future<> readSparseIndex();
  seastar::future<TSMIndexEntry*> getFullIndexEntry(const SeriesId128& seriesId);
  // Bulk prefetch: warm the full index cache for multiple series in parallel
  seastar::future<> prefetchFullIndexEntries(const std::vector<SeriesId128>& seriesIds);

  template <class T>
  seastar::future<> readSeries(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime, TSMResult<T> &results);
  template <class T>
  seastar::future<> readSeriesBatched(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime, TSMResult<T> &results);
  template <class T>
  seastar::future<> readBlock(TSMIndexBlock indexBlock, uint64_t startTime, uint64_t endTime, TSMResult<T> &results);
  template <class T>
  seastar::future<> readBlockBatch(const BlockBatch& batch, uint64_t startTime, uint64_t endTime, TSMResult<T> &results);
  std::optional<TSMValueType> getSeriesType(const SeriesId128& seriesId);

  // Block batching utilities
  std::vector<BlockBatch> groupContiguousBlocks(const std::vector<TSMIndexBlock>& blocks) const;
  template <class T>
  std::unique_ptr<TSMBlock<T>> decodeBlock(Slice& blockSlice, uint32_t blockSize, uint64_t startTime, uint64_t endTime);

  // Phase 1.1: New methods for streaming block access
  // Get index blocks for a series without reading data (for lazy loading)
  std::vector<TSMIndexBlock> getSeriesBlocks(const SeriesId128& seriesId) const;

  // Read a single block and return it (for on-demand loading)
  template <class T>
  seastar::future<std::unique_ptr<TSMBlock<T>>> readSingleBlock(const TSMIndexBlock &indexBlock, uint64_t startTime, uint64_t endTime);

  // Phase 2: Read compressed block bytes directly (zero-copy transfer)
  seastar::future<seastar::temporary_buffer<uint8_t>> readCompressedBlock(const TSMIndexBlock &indexBlock);

  // Get all series IDs in this file (for compaction)
  std::vector<SeriesId128> getSeriesIds() const {
    std::vector<SeriesId128> ids;
    ids.reserve(sparseIndex.size());
    for (const auto& [id, entry] : sparseIndex) {
      ids.push_back(id);
    }
    return ids;
  }
  
  // Get file size for compaction planning
  uint64_t getFileSize() const { return length; }

  template <class T>
  static constexpr TSMValueType getValueType(){
    if constexpr (std::is_same_v<T, double>) {
      return TSMValueType::Float;
    } else if constexpr (std::is_same_v<T, bool>) {
      return TSMValueType::Boolean;
    } else if constexpr (std::is_same_v<T, std::string>) {
      return TSMValueType::String;
    } else {
      static_assert(sizeof(T) == 0, "Unsupported TSM value type");
    }
  }
  
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
  
  // Pushdown aggregation: decode blocks and fold directly into BlockAggregator
  // instead of materialising TSMResult. Returns the number of points aggregated.
  // Only works for Float series; returns 0 for other types.
  seastar::future<size_t> aggregateSeries(
    const SeriesId128& seriesId,
    uint64_t startTime,
    uint64_t endTime,
    tsdb::BlockAggregator& aggregator
  );

  // Delete tombstone file after compaction
  seastar::future<> deleteTombstoneFile();
};

#endif
