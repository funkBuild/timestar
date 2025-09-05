#ifndef __WAL_H_INCLUDED__
#define __WAL_H_INCLUDED__

#include "tsdb_value.hpp"
#include "memory_store.hpp"
#include "slice_buffer.hpp"

#include <memory>
#include <fstream>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/iostream.hh>

class MemoryStore;

enum class WALType { Write = 0, Delete, DeleteRange, Close };
enum class WALValueType { Float = 0, Boolean, String };


class WAL {
private:
  static constexpr size_t WAL_BLOCK_SIZE = 65536;  // 64KB for better SSD performance
  static constexpr size_t WAL_ALIGNMENT = 4096;   // 4KB alignment for modern drives
  
  unsigned int sequenceNumber;
  seastar::file walFile;
  seastar::temporary_buffer<char> writeBuffer;
  size_t bufferPos = 0;
  uint64_t filePos = 0;
  std::atomic<size_t> currentSize{0};
  bool requiresImmediateFlush = false;  // Batch writes for better performance
  seastar::timer<> flushTimer;           // Timer for periodic flushes
  static constexpr auto FLUSH_INTERVAL = std::chrono::milliseconds(100);  // Flush every 100ms
  
  // Helper function to align up
  static size_t align_up(size_t value, size_t alignment) {
    return ((value + alignment - 1) / alignment) * alignment;
  }
  
  seastar::future<> flushBlock();

public:
  WAL(unsigned int _sequenceNumber);
  ~WAL();  // Destructor to ensure cleanup
  seastar::future<> init(MemoryStore *store);
  template <class T>
  seastar::future<> insert(TSDBInsert<T> &insertRequest);
  template <class T>
  size_t estimateInsertSize(TSDBInsert<T> &insertRequest);  // Calculates exact size by encoding data
  
  // Batch insert for multiple series at once
  template <class T>
  seastar::future<> insertBatch(std::vector<TSDBInsert<T>>& insertRequests);
  
  // Delete operations
  seastar::future<> deleteRange(const std::string& seriesKey,
                                uint64_t startTime,
                                uint64_t endTime);
  
  seastar::future<> close();
  seastar::future<> finalFlush();  // Ensure all data is written
  seastar::future<unsigned long> size();
  size_t getCurrentSize() const { return currentSize.load(); }
  seastar::future<> remove();
  
  // Configuration methods
  void setImmediateFlush(bool immediate) { requiresImmediateFlush = immediate; }


  static std::string sequenceNumberToFilename(unsigned int sequenceNumber);
  static void remove(unsigned int sequenceNumber);
};

class WALReader {
private:
  std::string filename;
  seastar::file walFile;
  size_t length = 0;

  template <class T>
  TSDBInsert<T> readSeries(Slice &walSlice, std::string &seriesId);
public:
  WALReader(std::string filename);
  seastar::future<> readAll(MemoryStore *store);
};

#endif
