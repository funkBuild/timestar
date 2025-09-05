#ifndef __TSM_WRITER_H_INCLUDED__
#define __TSM_WRITER_H_INCLUDED__

#include "memory_store.hpp"
#include "aligned_buffer.hpp"
#include "tsm.hpp"

#include <iostream>
#include <vector>
#include <string>
#include <seastar/core/shared_ptr.hh>

// From InfluxDB, might not be optimal
#define MaxPointsPerBlock 10000

class TSMWriter {
private:
  AlignedBuffer buffer;
  std::vector<TSMIndexEntry> indexEntries;
  std::string filename;

  void writeHeader();
public:
  TSMWriter(std::string _filename);

  template <class T>
  void writeSeries(TSMValueType seriesType, const std::string &seriesId, const std::vector<uint64_t> &timestamps, const std::vector<T> &values);
  template <class T>
  void writeBlock(TSMValueType seriesType, const std::string &seriesId, const std::vector<uint64_t> &timestamps, const std::vector<T> &values, TSMIndexEntry &indexEntry);
  void writeIndex();
  void writeIndexBlock(const std::vector<uint64_t> &timestamps, TSMIndexEntry &indexEntry, size_t blockStartOffset);
  void close();

  static void run(seastar::shared_ptr<MemoryStore> store, std::string filename);
};

#endif
