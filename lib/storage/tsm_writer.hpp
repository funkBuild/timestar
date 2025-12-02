#ifndef __TSM_WRITER_H_INCLUDED__
#define __TSM_WRITER_H_INCLUDED__

#include "memory_store.hpp"
#include "aligned_buffer.hpp"
#include "tsm.hpp"
#include "series_id.hpp"

#include <iostream>
#include <vector>
#include <string>
#include <map>
#include <seastar/core/shared_ptr.hh>

// Optimized via benchmark: 3000 provides 25% faster queries with equal insert performance
// 3000 points × 16 bytes = 48KB fits perfectly in L2 cache (256-512KB)
#define MaxPointsPerBlock 3000

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
  void close();

  static void run(seastar::shared_ptr<MemoryStore> store, std::string filename);
};

#endif
