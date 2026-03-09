#ifndef WAL_H_INCLUDED
#define WAL_H_INCLUDED

#include "memory_store.hpp"
#include "series_id.hpp"
#include "slice_buffer.hpp"
#include "timestar_value.hpp"

#include <algorithm>
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

class AlignedBuffer;
class MemoryStore;

enum class WALType { Write = 0, Delete, DeleteRange, Close };
enum class WALValueType { Float = 0, Boolean, String, Integer };

enum class WALInsertResult {
    Success,
    RolloverNeeded
};

// Structure to track timing information for WAL operations
struct WALTimingInfo {
    std::chrono::microseconds compressionTime{0};
    std::chrono::microseconds walWriteTime{0};
    int walWriteCount{0};
};

// Adaptive compression ratio tracking for WAL size estimation.
// Updated after each encodeInsertEntry() using exponential moving average (EMA).
// Per-WAL instance (thread-local in Seastar's shard-per-core model, no atomics needed).
struct CompressionStats {
    // Initial ratios are conservative (slightly overestimate) to avoid
    // underestimation before sufficient samples are collected.
    double timestampRatio = 0.25;   // Timestamps: typically ~15-20% of raw
    double floatRatio = 0.30;       // Floats (ALP/Gorilla): typically ~20-25%
    double boolRatio = 0.05;        // Bools (bit-packing): typically ~1.6%
    double stringRatio = 0.40;      // Strings (Snappy): typically ~10-30%
    double integerRatio = 0.25;     // Integers (ZigZag + FFOR): typically ~15-25%
    size_t sampleCount = 0;         // Number of encodes observed

    // EMA smoothing factor. Higher alpha = faster convergence but more noise.
    // 0.15 converges in ~10-15 samples while remaining stable.
    static constexpr double ALPHA = 0.15;

    // Minimum floor ratios to prevent dangerously low estimates.
    // Even highly compressible data should never estimate below these.
    static constexpr double MIN_TIMESTAMP_RATIO = 0.03;
    static constexpr double MIN_FLOAT_RATIO     = 0.05;
    static constexpr double MIN_BOOL_RATIO      = 0.01;
    static constexpr double MIN_STRING_RATIO    = 0.05;
    static constexpr double MIN_INTEGER_RATIO   = 0.03;

    // Safety margin applied to all estimates to avoid underestimation
    static constexpr double SAFETY_MARGIN = 1.1;

    void updateTimestamp(size_t rawSize, size_t encodedSize) {
        if (rawSize == 0) [[unlikely]] return;
        double observed = static_cast<double>(encodedSize) / static_cast<double>(rawSize);
        timestampRatio = std::clamp(
            timestampRatio * (1.0 - ALPHA) + observed * ALPHA,
            MIN_TIMESTAMP_RATIO, 1.0);
        ++sampleCount;
    }

    void updateFloat(size_t rawSize, size_t encodedSize) {
        if (rawSize == 0) [[unlikely]] return;
        double observed = static_cast<double>(encodedSize) / static_cast<double>(rawSize);
        floatRatio = std::clamp(
            floatRatio * (1.0 - ALPHA) + observed * ALPHA,
            MIN_FLOAT_RATIO, 1.0);
        ++sampleCount;
    }

    void updateBool(size_t rawSize, size_t encodedSize) {
        if (rawSize == 0) [[unlikely]] return;
        double observed = static_cast<double>(encodedSize) / static_cast<double>(rawSize);
        boolRatio = std::clamp(
            boolRatio * (1.0 - ALPHA) + observed * ALPHA,
            MIN_BOOL_RATIO, 1.0);
        ++sampleCount;
    }

    void updateString(size_t rawSize, size_t encodedSize) {
        if (rawSize == 0) [[unlikely]] return;
        double observed = static_cast<double>(encodedSize) / static_cast<double>(rawSize);
        stringRatio = std::clamp(
            stringRatio * (1.0 - ALPHA) + observed * ALPHA,
            MIN_STRING_RATIO, 1.0);
        ++sampleCount;
    }

    void updateInteger(size_t rawSize, size_t encodedSize) {
        if (rawSize == 0) [[unlikely]] return;
        double observed = static_cast<double>(encodedSize) / static_cast<double>(rawSize);
        integerRatio = std::clamp(
            integerRatio * (1.0 - ALPHA) + observed * ALPHA,
            MIN_INTEGER_RATIO, 1.0);
        ++sampleCount;
    }
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
  // No atomic needed: WAL is a per-shard object in Seastar's shard-per-core model,
  // only accessed from a single thread.
  size_t currentSize = 0;

  // DMA alignment tracking: Seastar's file_data_sink_impl requires that every
  // write position is aligned to disk_write_dma_alignment (typically 4096).
  // When we call output_stream::flush(), a partial buffer is sent to the sink,
  // which advances _pos by a non-aligned amount.  Subsequent writes then fail
  // the alignment assertion.  We avoid this by padding the stream to alignment
  // before every explicit flush().
  size_t _dma_alignment = 4096;
  size_t _unflushed_bytes = 0; // bytes written to output_stream since last flush

  // Write padding zeros to align the stream position to DMA boundary before flushing
  seastar::future<> padToAlignment();

  // Flush controls
  bool requiresImmediateFlush = false; // if true, flush after each write/batch

  // Stream serialization to prevent concurrent access
  seastar::semaphore _io_sem{1};
  seastar::gate _io_gate;
  
  // Close guard to prevent double-close
  bool _closed = false;

  // Adaptive compression ratio tracking. Updated after each encodeInsertEntry()
  // to converge estimates toward actual encoded sizes. Per-WAL instance,
  // no synchronization needed (Seastar shard-per-core model).
  CompressionStats _compressionStats;

  // Flush buffered bytes and ensure durability (fdatasync)
  seastar::future<> flushBlock();

public:
  WAL(unsigned int _sequenceNumber);
  ~WAL(); // Ensure caller invoked close()/finalFlush() before destruction

  seastar::future<> init(MemoryStore *store, bool isRecovery = false);

  // Insert a single series write
  template <class T> seastar::future<WALInsertResult> insert(TimeStarInsert<T> &insertRequest);

  // Lightweight upper-bound size estimation for capacity/rollover decisions
  // (does not perform actual encoding)
  template <class T> size_t estimateInsertSize(TimeStarInsert<T> &insertRequest);

  // Batch insert for multiple series at once (coalesces I/O)
  template <class T>
  seastar::future<WALInsertResult> insertBatch(std::vector<TimeStarInsert<T>> &insertRequests);

private:
  // Encode a single insert entry (header + payload + CRC) into the provided
  // buffer.  Used by both insert() and insertBatch() to avoid duplicating
  // the encoding logic.  The buffer is appended to; the caller is responsible
  // for pre-allocating sufficient capacity.  Non-static: updates
  // _compressionStats with observed compression ratios after encoding.
  template <class T>
  void encodeInsertEntry(AlignedBuffer &buffer, TimeStarInsert<T> &insertRequest);

public:

  // Delete range operation
  seastar::future<> deleteRange(const SeriesId128 &seriesId, uint64_t startTime,
                                uint64_t endTime);

  // Lifecycle
  seastar::future<> close();
  seastar::future<> finalFlush();        // ensure all data is written & durable
  seastar::future<unsigned long> size(); // physical file size
  size_t getCurrentSize() const { return currentSize; }
  seastar::future<> remove(); // remove this WAL file

  // Configuration
  void setImmediateFlush(bool immediate) { requiresImmediateFlush = immediate; }

  // Utilities
  static std::string sequenceNumberToFilename(unsigned int sequenceNumber);
  static seastar::future<> remove(unsigned int sequenceNumber);
};

class WALReader {
private:
  std::string filename;
  seastar::file walFile;
  size_t length = 0;

  template <class T>
  TimeStarInsert<T> readSeries(Slice &walSlice, const std::string &seriesKey);

public:
  WALReader(std::string filename);
  seastar::future<> readAll(MemoryStore *store);
};

#endif // WAL_H_INCLUDED
