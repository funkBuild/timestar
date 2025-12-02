#ifndef __WAL_H_INCLUDED__
#define __WAL_H_INCLUDED__

#include "memory_store.hpp"
#include "series_id.hpp"
#include "slice_buffer.hpp"
#include "tsdb_value.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_timeout.hh>

class MemoryStore;

enum class WALType { Write = 0, Delete, DeleteRange, Close };
enum class WALValueType { Float = 0, Boolean, String };

// Structure to track timing information for WAL operations
struct WALTimingInfo {
    std::chrono::microseconds compressionTime{0};
    std::chrono::microseconds walWriteTime{0};
    int walWriteCount{0};
};

class WAL {
private:
  // WAL sizing policy
  static constexpr size_t MAX_WAL_SIZE = 16 * 1024 * 1024;                // 16 MiB per segment
  // Stream buffer size (controls batching of small appends before a flush hits the device)
  static constexpr size_t STREAM_BUFFER_SIZE = 1 * 1024 * 1024;           // 1 MiB

  // Identity & file
  unsigned int sequenceNumber;
  seastar::file walFile;

  // Streamed, unaligned I/O (buffered internally by Seastar)
  std::optional<seastar::output_stream<char>> out;

  // Position & accounting
  uint64_t filePos = 0;
  std::atomic<size_t> currentSize{0};


  // Flush controls
  bool requiresImmediateFlush = false; // if true, flush after each write/batch

  // Stream serialization to prevent concurrent access
  seastar::semaphore _io_sem{1};
  seastar::gate _io_gate;
  
  // Close guard to prevent double-close
  bool _closed = false;

  // Legacy leftover counter (kept to avoid changing user code paths/logging);
  // not used by the streaming implementation but referenced in wal.cpp
  // destructor.
  size_t bufferPos = 0;

  // Flush buffered bytes and ensure durability (fdatasync)
  seastar::future<> flushBlock();

public:
  WAL(unsigned int _sequenceNumber);
  ~WAL(); // Ensure caller invoked close()/finalFlush() before destruction

  seastar::future<> init(MemoryStore *store, bool isRecovery = false);

  // Insert a single series write
  template <class T> seastar::future<> insert(TSDBInsert<T> &insertRequest);

  // Exact on-disk size estimation for capacity/rollover decisions
  template <class T> size_t estimateInsertSize(TSDBInsert<T> &insertRequest);

  // Batch insert for multiple series at once (coalesces I/O)
  template <class T>
  seastar::future<> insertBatch(std::vector<TSDBInsert<T>> &insertRequests);

  // Delete range operation
  seastar::future<> deleteRange(const SeriesId128 &seriesId, uint64_t startTime,
                                uint64_t endTime);

  // Lifecycle
  seastar::future<> close();
  seastar::future<> finalFlush();        // ensure all data is written & durable
  seastar::future<unsigned long> size(); // physical file size
  size_t getCurrentSize() const { return currentSize.load(); }
  seastar::future<> remove(); // remove this WAL file

  // Configuration
  void setImmediateFlush(bool immediate) { requiresImmediateFlush = immediate; }

  // Utilities
  static std::string sequenceNumberToFilename(unsigned int sequenceNumber);
  static void remove(unsigned int sequenceNumber);
};

class WALReader {
private:
  std::string filename;
  seastar::file walFile;
  size_t length = 0;

  template <class T>
  TSDBInsert<T> readSeries(Slice &walSlice, const std::string &seriesKey);

public:
  WALReader(std::string filename);
  seastar::future<> readAll(MemoryStore *store);
};

#endif // __WAL_H_INCLUDED__
