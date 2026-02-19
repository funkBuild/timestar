#ifndef TSM_WRITER_H_INCLUDED
#define TSM_WRITER_H_INCLUDED

#include "memory_store.hpp"
#include "aligned_buffer.hpp"
#include "tsm.hpp"
#include "series_id.hpp"
#include "tsdb_config.hpp"

#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future.hh>
#include <seastar/core/coroutine.hh>

// Optimized via benchmark: 3000 provides 25% faster queries with equal insert performance
// 3000 points × 16 bytes = 48KB fits perfectly in L2 cache (256-512KB)
// Configurable via [storage] max_points_per_block in TOML config.
#define MaxPointsPerBlock (tsdb::config().storage.max_points_per_block)

class TSMWriter {
private:
  AlignedBuffer buffer;
  // std::map is optimal for this use case (small dataset, maintains sorted order)
  std::map<SeriesId128, TSMIndexEntry> indexEntries;
  std::string filename;

  void writeHeader();
public:
  TSMWriter(std::string _filename);

  template <class T>
  void writeSeries(TSMValueType seriesType, const SeriesId128 &seriesId, const std::vector<uint64_t> &timestamps, const std::vector<T> &values);
  template <class T>
  void writeBlock(TSMValueType seriesType, const SeriesId128 &seriesId, const std::vector<uint64_t> &timestamps, const std::vector<T> &values, TSMIndexEntry &indexEntry);

  // Phase 3.2: Move semantics overloads for zero-copy writes
  template <class T>
  void writeSeriesDirect(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<T> &&values);
  template <class T>
  void writeBlockDirect(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<T> &&values, TSMIndexEntry &indexEntry);

  // Phase 2: Write compressed block bytes directly (zero-copy transfer)
  void writeCompressedBlock(TSMValueType seriesType, const SeriesId128 &seriesId,
                           seastar::temporary_buffer<uint8_t> &&compressedData,
                           uint64_t minTime, uint64_t maxTime);

  void writeIndex();

  // Phase 4A: Parallel index building
  void writeIndexParallel();

  void writeIndexBlock(const std::vector<uint64_t> &timestamps, TSMIndexEntry &indexEntry, size_t blockStartOffset);

  // Blocking close using POSIX I/O (for use in tests or seastar::async contexts)
  void close();

  // Async close using Seastar DMA I/O (non-blocking, for use on reactor thread)
  // Writes buffer to disk using open_file_dma + dma_write + flush + close.
  // Produces byte-identical output to close().
  seastar::future<> closeDMA();

  // Blocking run: builds TSM file in memory, writes via POSIX I/O (for tests)
  static void run(seastar::shared_ptr<MemoryStore> store, std::string filename);

  // Async run: builds TSM file in memory, writes via Seastar DMA I/O (for production)
  static seastar::future<> runAsync(seastar::shared_ptr<MemoryStore> store, std::string filename);
};

#endif
