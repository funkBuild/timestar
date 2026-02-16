// wal.cpp — stream-based, unaligned I/O version (fixed build errors)

#include "wal.hpp"

#include "aligned_buffer.hpp"
#include "bool_encoder.hpp"
#include "crc32.hpp"
#include "float_encoder.hpp"
#include "integer_encoder.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include "series_id.hpp"
#include "string_encoder.hpp"
#include "tsm.hpp"

#include <chrono>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/timer.hh>

namespace fs = std::filesystem;

// ------------------------ WAL ------------------------

WAL::WAL(unsigned int _sequenceNumber) : sequenceNumber(_sequenceNumber) {}

WAL::~WAL() {
  // Note: Can't do async ops in destructor. Warn if anything left buffered.
  if (bufferPos > 0) {
    tsdb::wal_log.warn("WAL destructor called with unflushed data ({} bytes)",
                       bufferPos);
  }
}

seastar::future<> WAL::init(MemoryStore * /*store*/, bool isRecovery) {
  std::string filename = sequenceNumberToFilename(sequenceNumber);

  // Ensure directory exists
  try {
    fs::create_directories(fs::path(filename).parent_path());
  } catch (...) {
    // best-effort
  }

  bool fileExisted = fs::exists(filename);

  std::string_view filenameView{filename};

  // Determine open flags based on whether this is recovery or fresh creation
  seastar::open_flags openFlags;
  if (isRecovery) {
    // Recovery mode: open existing file for append, don't truncate
    if (!fileExisted) {
      tsdb::wal_log.error("WAL file {} does not exist for recovery", filename);
      throw std::runtime_error("WAL file not found for recovery");
    }
    openFlags = seastar::open_flags::rw | seastar::open_flags::create;
    tsdb::wal_log.debug("Opening WAL {} for recovery", filename);
  } else {
    // Fresh creation mode: always truncate to start with empty file
    if (fileExisted) {
      tsdb::wal_log.warn("WAL file {} already exists when creating new WAL. "
                         "Truncating to start fresh.",
                         filename);
    }
    openFlags = seastar::open_flags::rw | seastar::open_flags::create |
                seastar::open_flags::truncate;
    tsdb::wal_log.debug("Creating fresh WAL {}", filename);
  }

  walFile = co_await seastar::open_file_dma(filenameView, openFlags);

  if (!walFile) {
    throw std::runtime_error("Failed to open WAL file: " + filename);
  }

  tsdb::wal_log.debug("WAL file opened: {}", filename);

  // Get current file size
  filePos = co_await walFile.size();
  currentSize = filePos;

  if (!isRecovery && currentSize > 0) {
    tsdb::wal_log.error(
        "Fresh WAL {} has non-zero size {} after truncate - this is unexpected",
        filename, currentSize);
  }

  // Capture DMA alignment from the file so we can pad before flushing
  _dma_alignment = walFile.disk_write_dma_alignment();
  _unflushed_bytes = 0;

  // Build buffered output stream.  The buffer size MUST be a multiple of
  // _dma_alignment so that automatic full-buffer flushes always land on
  // aligned positions.  We use file_output_stream_options to set this
  // explicitly (default 65536 is fine, it's a multiple of 4096).
  seastar::file_output_stream_options opts;
  opts.buffer_size = 65536; // 64 KiB, guaranteed multiple of DMA alignment
  auto s = co_await seastar::make_file_output_stream(walFile, opts);
  out.emplace(std::move(s));

  tsdb::wal_log.debug("WAL stream init: pos={}, dma_alignment={}", filePos, _dma_alignment);
}

std::string WAL::sequenceNumberToFilename(unsigned int sequenceNumber) {
  std::string path = "shard_" + std::to_string(seastar::this_shard_id()) + "/";
  std::string sequenceNumStr = std::to_string(sequenceNumber);
  size_t padLen = sequenceNumStr.length() >= 10 ? 0 : 10 - sequenceNumStr.length();
  std::string filename = path + std::string(padLen, '0')
                                    .append(sequenceNumStr)
                                    .append(".wal");
  return filename;
}

seastar::future<> WAL::finalFlush() {
  if (!out)
    co_return;
  try {
    // Pad to DMA alignment before the final flush
    co_await padToAlignment();
    co_await out->flush(); // drains buffers and fsyncs the sink
    _unflushed_bytes = 0;
    tsdb::wal_log.debug("WAL final flush completed, filePos={}", filePos);
  } catch (const std::exception &e) {
    tsdb::wal_log.error("WAL final flush error: {}", e.what());
  }
}

seastar::future<> WAL::close() {
  // Prevent double-close
  if (_closed || !walFile) {
    tsdb::wal_log.debug("WAL seq={} already closed or invalid file",
                        sequenceNumber);
    co_return;
  }

  _closed = true;
  tsdb::wal_log.debug("WAL seq={} starting close", sequenceNumber);

  // Wait for all in-flight operations to complete before closing
  tsdb::wal_log.debug("WAL seq={} waiting for in-flight operations to complete",
                      sequenceNumber);
  co_await _io_gate.close();
  tsdb::wal_log.debug("WAL seq={} all operations completed", sequenceNumber);

  // Close the output stream before destroying it
  // Note: closing the stream also closes the underlying file
  if (out) {
    tsdb::wal_log.debug("WAL seq={} closing output stream", sequenceNumber);
    try {
      co_await out->close();
      tsdb::wal_log.debug("WAL seq={} output stream and file closed", sequenceNumber);
    } catch (const std::exception &e) {
      tsdb::wal_log.error("WAL seq={} output stream close error: {}",
                          sequenceNumber, e.what());
    }
    out.reset();
  } else if (walFile) {
    // Only close file directly if stream wasn't created
    tsdb::wal_log.debug("WAL seq={} closing file directly", sequenceNumber);
    try {
      co_await walFile.flush();
      co_await walFile.close();
      tsdb::wal_log.debug("WAL seq={} file close completed", sequenceNumber);
    } catch (const std::exception &e) {
      tsdb::wal_log.error("WAL seq={} file close error: {}", sequenceNumber,
                          e.what());
      // Don't rethrow during shutdown
    }
  }

  tsdb::wal_log.debug("WAL seq={} closed successfully", sequenceNumber);
}

seastar::future<unsigned long> WAL::size() {
  auto s = co_await walFile.size();
  co_return static_cast<unsigned long>(s);
}

seastar::future<> WAL::remove() {
  // Ensure file is closed before removal to avoid dangling fd
  if (!_closed) {
    co_await close();
  }
  std::string filename = WAL::sequenceNumberToFilename(sequenceNumber);
  co_await seastar::remove_file(filename);
}

// Write structured padding so that the total bytes written to the output stream
// since the last flush are a multiple of _dma_alignment.  This ensures the
// underlying file_data_sink_impl's write position stays aligned after the
// flush, preventing the DMA alignment assertion failure on subsequent writes.
//
// Padding format (Issue #11 fix):
//   [uint32_t  0]          <-- padding marker (entryLength == 0)
//   [uint32_t  skipBytes]  <-- number of additional zero bytes following
//   [skipBytes zero bytes]
//
// Total padding is always >= 8 bytes.  If the natural padding would be less
// than 8, we bump up by one full alignment block so the header always fits.
seastar::future<> WAL::padToAlignment() {
  if (!out || _dma_alignment <= 1)
    co_return;

  size_t remainder = _unflushed_bytes % _dma_alignment;
  if (remainder == 0)
    co_return; // already aligned

  size_t padding = _dma_alignment - remainder;

  // Ensure at least 8 bytes so the structured header (marker + skipBytes)
  // always fits.
  if (padding < 8) {
    padding += _dma_alignment;
  }

  // Write the 8-byte header: [paddingMarker=0][skipBytes]
  const uint32_t paddingMarker = 0;
  const uint32_t skipBytes = static_cast<uint32_t>(padding - 8);
  co_await out->write(reinterpret_cast<const char *>(&paddingMarker),
                      sizeof(paddingMarker));
  co_await out->write(reinterpret_cast<const char *>(&skipBytes),
                      sizeof(skipBytes));

  // Write the remaining zero bytes.
  if (skipBytes > 0) {
    char zeros[4096] = {};
    size_t written = 0;
    while (written < skipBytes) {
      size_t chunk =
          std::min(static_cast<size_t>(skipBytes) - written, sizeof(zeros));
      co_await out->write(zeros, chunk);
      written += chunk;
    }
  }

  _unflushed_bytes += padding;
  filePos += padding;
  currentSize += padding;
}

// Flush pending buffered bytes in the output stream
seastar::future<> WAL::flushBlock() {
  if (!out)
    co_return;
  try {
    auto startTime = std::chrono::steady_clock::now();

    // Pad to DMA alignment before flushing to keep file sink position aligned
    co_await padToAlignment();
    co_await out->flush();
    _unflushed_bytes = 0; // reset after successful flush

    auto endTime = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(endTime -
                                                                    startTime)
                  .count();
    LOG_INSERT_PATH(tsdb::wal_log, debug, "WAL stream flush: pos={}, took={}ms",
                    filePos, ms);
    if (ms > 100) {
      tsdb::wal_log.warn("WAL flush took {}ms - potential I/O bottleneck", ms);
    }
  } catch (const std::exception &e) {
    tsdb::wal_log.error("WAL flush error: {}", e.what());
  }
}

template <class T>
size_t WAL::estimateInsertSize(TSDBInsert<T> &insertRequest) {
  // Lightweight upper-bound estimation without performing full encoding.
  // This avoids the cost of encoding timestamps and values just to measure
  // their sizes. The actual encoded data is always <= the worst-case sizes
  // computed here, so this is safe for rollover/capacity decisions.
  size_t estimatedSize = 0;

  estimatedSize += 4;  // length prefix
  estimatedSize += 4;  // CRC32 checksum
  estimatedSize += 1;  // WAL type
  estimatedSize += 16; // SeriesId128 (fixed 16 bytes)

  // Series key string (length-prefixed)
  std::string seriesKey = insertRequest.seriesKey();
  estimatedSize += 4 + seriesKey.size();

  estimatedSize += 1;  // value type
  estimatedSize += 4;  // # of timestamps

  // Timestamps: worst case is uncompressed (count * sizeof(uint64_t)),
  // plus the 4-byte encoded-size prefix
  const size_t count = insertRequest.timestamps.size();
  estimatedSize += 4 + count * sizeof(uint64_t);

  // Values: worst case depends on type, plus the 4-byte encoded-size prefix
  estimatedSize += 4;
  if constexpr (std::is_same<T, double>::value) {
    // Worst case: uncompressed doubles
    estimatedSize += count * sizeof(double);
  } else if constexpr (std::is_same<T, bool>::value) {
    // Worst case: one byte per bool
    estimatedSize += count * sizeof(uint8_t);
  } else if constexpr (std::is_same<T, std::string>::value) {
    // Worst case: 4-byte varint length prefix + full string data per entry
    // (Snappy can expand slightly, so account for that too)
    for (const auto &s : insertRequest.values) {
      estimatedSize += 4 + s.size();
    }
  }

  return estimatedSize;
}

template <class T> seastar::future<> WAL::insert(TSDBInsert<T> &insertRequest) {
  // --- Encoding phase (no lock needed) ---
  // Build the payload (everything after the length prefix and CRC)
  AlignedBuffer payload;

  payload.write((uint8_t)WALType::Write);

  // Store SeriesId128 (fixed 16 bytes)
  SeriesId128 seriesId = insertRequest.seriesId128();
  std::string seriesIdBytes = seriesId.toBytes();
  payload.write(seriesIdBytes);

  // Store series key string for recovery (length-prefixed)
  std::string seriesKey = insertRequest.seriesKey();
  payload.write((uint32_t)seriesKey.size());
  payload.write(seriesKey);

  // Value type
  payload.write((uint8_t)TSM::getValueType<T>());

  // Num of timestamps
  payload.write((uint32_t)insertRequest.timestamps.size());

  // Encoded timestamps
  {
    AlignedBuffer encodedTimestamps =
        IntegerEncoder::encode(insertRequest.timestamps);
    payload.write((uint32_t)encodedTimestamps.size());
    payload.write(encodedTimestamps);
  }

  // Encoded values
  if constexpr (std::is_same<T, double>::value) {
    CompressedBuffer encodedFloats = FloatEncoder::encode(insertRequest.values);
    payload.write((uint32_t)encodedFloats.size());
    payload.write(encodedFloats);
  } else if constexpr (std::is_same<T, bool>::value) {
    AlignedBuffer encodedBools = BoolEncoder::encode(insertRequest.values);
    payload.write((uint32_t)encodedBools.size());
    payload.write(encodedBools);
  } else if constexpr (std::is_same<T, std::string>::value) {
    AlignedBuffer encodedStrings = StringEncoder::encode(insertRequest.values);
    payload.write((uint32_t)encodedStrings.size());
    payload.write(encodedStrings);
  } else {
    throw std::runtime_error("Unsupported data type");
  }

  // Compute CRC32 over the payload
  uint32_t crc = CRC32::compute(payload.data.data(), payload.size());

  // Assemble final buffer: [entryLength][crc32][payload]
  // entryLength = 4 (CRC) + payload size
  AlignedBuffer buffer;
  const uint32_t entryLength = static_cast<uint32_t>(4 + payload.size());
  buffer.write(entryLength);
  buffer.write(crc);
  buffer.write(payload);

  const size_t dataSize = buffer.size();

  // --- I/O phase (under lock) ---
  // Hold the gate BEFORE the semaphore so that WAL::close() (which calls
  // _io_gate.close()) will wait for coroutines queued on _io_sem rather
  // than racing past them and closing the stream while they're pending.
  auto gate_holder = _io_gate.hold();
  auto units = co_await seastar::get_units(_io_sem, 1);

  LOG_INSERT_PATH(tsdb::wal_log, debug,
                  "WAL::insert - dataSize={}, entryLength={}, currentSize={}",
                  dataSize, entryLength, currentSize);

  // Respect the 16MiB WAL limit BEFORE writing
  const size_t projectedSize = currentSize + dataSize;
  if (projectedSize > MAX_WAL_SIZE) {
    tsdb::wal_log.debug("WAL::insert - Would exceed 16MB limit (current={}, "
                        "dataSize={}, projected={}), signaling rollover needed",
                        currentSize, dataSize, projectedSize);
    throw std::runtime_error("WAL rollover needed");
  }

  // Stream write
  try {
    if (out) {
      co_await out->write(reinterpret_cast<const char *>(buffer.data.data()),
                          dataSize);
    } else {
      throw std::runtime_error("WAL output stream is null");
    }
    // Update positions and size
    filePos += dataSize;
    currentSize += dataSize;
    _unflushed_bytes += dataSize;

    // Only flush immediately if configured for immediate mode
    if (requiresImmediateFlush) {
      co_await padToAlignment();
      co_await out->flush();
      _unflushed_bytes = 0;
    }
  } catch (const std::exception &e) {
    tsdb::wal_log.error("WAL::insert write failed: {}", e.what());
    throw;
  }
}

template <class T>
seastar::future<> WAL::insertBatch(std::vector<TSDBInsert<T>> &insertRequests) {
  if (insertRequests.empty()) {
    co_return;
  }
  if (insertRequests.size() == 1) {
    co_await insert(insertRequests[0]);
    co_return;
  }

  // --- Encoding phase (no lock needed) ---
  // Prepare all buffers first — encoding, compression, and CRC are CPU-bound
  // and can run concurrently with other WAL operations.
  std::vector<AlignedBuffer> buffers;
  buffers.reserve(insertRequests.size());
  size_t totalSize = 0;

  for (auto &insertRequest : insertRequests) {
    // Build the payload first (everything after length prefix and CRC)
    AlignedBuffer payload;

    payload.write((uint8_t)WALType::Write);

    // Store SeriesId128 (fixed 16 bytes)
    SeriesId128 seriesId = insertRequest.seriesId128();
    std::string seriesIdBytes = seriesId.toBytes();
    payload.write(seriesIdBytes);

    // Store series key string for recovery (length-prefixed)
    std::string seriesKey = insertRequest.seriesKey();
    payload.write((uint32_t)seriesKey.size());
    payload.write(seriesKey);

    payload.write((uint8_t)TSM::getValueType<T>());
    payload.write((uint32_t)insertRequest.timestamps.size());

    {
      AlignedBuffer encodedTimestamps =
          IntegerEncoder::encode(insertRequest.timestamps);
      payload.write((uint32_t)encodedTimestamps.size());
      payload.write(encodedTimestamps);
    }

    if constexpr (std::is_same<T, double>::value) {
      CompressedBuffer encodedFloats =
          FloatEncoder::encode(insertRequest.values);
      payload.write((uint32_t)encodedFloats.size());
      payload.write(encodedFloats);
    } else if constexpr (std::is_same<T, bool>::value) {
      AlignedBuffer encodedBools = BoolEncoder::encode(insertRequest.values);
      payload.write((uint32_t)encodedBools.size());
      payload.write(encodedBools);
    } else if constexpr (std::is_same<T, std::string>::value) {
      AlignedBuffer encodedStrings =
          StringEncoder::encode(insertRequest.values);
      payload.write((uint32_t)encodedStrings.size());
      payload.write(encodedStrings);
    }

    // Compute CRC32 over the payload
    uint32_t crc = CRC32::compute(payload.data.data(), payload.size());

    // Assemble final buffer: [entryLength][crc32][payload]
    AlignedBuffer buf;
    uint32_t entryLength = static_cast<uint32_t>(4 + payload.size());
    buf.write(entryLength);
    buf.write(crc);
    buf.write(payload);

    totalSize += buf.size();
    buffers.push_back(std::move(buf));
  }

  // --- I/O phase (under lock) ---
  // Hold the gate BEFORE the semaphore (see insert() for rationale).
  auto gate_holder = _io_gate.hold();
  auto units = co_await seastar::get_units(_io_sem, 1);

  LOG_INSERT_PATH(
      tsdb::wal_log, debug,
      "WAL::insertBatch - {} entries, total size={}, currentSize={}",
      buffers.size(), totalSize, currentSize);

  // WAL limit check (under lock so currentSize is authoritative)
  const size_t projected = currentSize + totalSize;
  if (projected > MAX_WAL_SIZE) {
    tsdb::wal_log.debug(
        "WAL::insertBatch - Would exceed 16MB limit (current={}, total={}, "
        "projected={}), signaling rollover",
        currentSize, totalSize, projected);
    throw std::runtime_error("WAL rollover needed");
  }

  try {
    if (out) {
      // Write all encoded buffers; output_stream will coalesce internally.
      // We write all entries before updating filePos/currentSize so that
      // if an exception is thrown mid-batch, accounting stays consistent.
      for (auto &b : buffers) {
        co_await out->write(reinterpret_cast<const char *>(b.data.data()),
                            b.size());
      }
    }
    // Only update positions atomically after ALL writes succeed
    filePos += totalSize;
    currentSize += totalSize;
    _unflushed_bytes += totalSize;

    // Only flush immediately if configured for immediate mode
    if (requiresImmediateFlush) {
      co_await padToAlignment();
      co_await out->flush();
      _unflushed_bytes = 0;
    }
  } catch (const std::exception &e) {
    tsdb::wal_log.error("WAL::insertBatch write failed: {}", e.what());
    throw;
  }
}

seastar::future<> WAL::deleteRange(const SeriesId128 &seriesId,
                                   uint64_t startTime, uint64_t endTime) {
  // --- Encoding phase (no lock needed) ---
  // Build the payload (everything after the length prefix and CRC)
  AlignedBuffer payload;

  // Type
  payload.write((uint8_t)WALType::DeleteRange);

  // Series ID (fixed 16 bytes)
  std::string seriesIdBytes = seriesId.toBytes();
  payload.write(seriesIdBytes);

  // Time range
  payload.write(startTime);
  payload.write(endTime);

  // Compute CRC32 over the payload
  uint32_t crc = CRC32::compute(payload.data.data(), payload.size());

  // Assemble final buffer: [entryLength][crc32][payload]
  AlignedBuffer buffer;
  uint32_t entryLength = static_cast<uint32_t>(4 + payload.size());
  buffer.write(entryLength);
  buffer.write(crc);
  buffer.write(payload);

  const size_t n = buffer.size();

  // --- I/O phase (under lock) ---
  // Hold the gate BEFORE the semaphore (see insert() for rationale).
  auto gate_holder = _io_gate.hold();
  auto units = co_await seastar::get_units(_io_sem, 1);

  try {
    if (out) {
      co_await out->write(reinterpret_cast<const char *>(buffer.data.data()),
                          n);
    }
    filePos += n;
    currentSize += n;
    _unflushed_bytes += n;

    // Only flush immediately if configured for immediate mode
    if (requiresImmediateFlush) {
      co_await padToAlignment();
      co_await out->flush();
      _unflushed_bytes = 0;
    }
  } catch (const std::exception &e) {
    tsdb::wal_log.error("WAL::deleteRange write failed: {}", e.what());
    throw;
  }

  LOG_INSERT_PATH(tsdb::wal_log, debug,
                  "WAL deleteRange written: series={}, startTime={}, "
                  "endTime={}, size={} bytes",
                  seriesId.toHex(), startTime, endTime, n);
}

seastar::future<> WAL::remove(unsigned int sequenceNumber) {
  std::string filename = WAL::sequenceNumberToFilename(sequenceNumber);

  try {
    co_await seastar::remove_file(filename);
    tsdb::wal_log.debug("WAL file {} deleted", filename);
  } catch (const std::exception &e) {
    // File may not exist; log but don't propagate
    tsdb::wal_log.debug("WAL file {} removal failed (may not exist): {}",
                        filename, e.what());
  }
}

// ------------------------ WALReader ------------------------

WALReader::WALReader(std::string _filename) : filename(std::move(_filename)) {}

seastar::future<> WALReader::readAll(MemoryStore *store) {
  std::string_view filenameView{filename};
  walFile =
      co_await seastar::open_file_dma(filenameView, seastar::open_flags::ro);

  if (!walFile) {
    tsdb::wal_log.error("Failed to open WAL file: {}", filename);
    throw std::runtime_error("Failed opening WAL file");
  }

  length = co_await walFile.size();
  if (length == 0) {
    tsdb::wal_log.info(
        "WAL recovery complete: 0 entries read, 0 partial entries discarded");
    co_await walFile.close();
    co_return;
  }

  tsdb::wal_log.info("WAL recovery starting for file {} with size {} bytes",
                     filename, length);

  size_t entriesRead = 0;
  size_t partialEntries = 0;
  bool stopRecovery = false;

  // Stream the file; no aligned over-reads
  auto in = seastar::make_file_input_stream(walFile);

  // read_exact helper using read_up_to (no trim_front needed)
  auto read_exact = [&in](void *dst, size_t n) -> seastar::future<bool> {
    size_t got = 0;
    while (got < n) {
      auto chunk = co_await in.read_up_to(n - got);
      if (chunk.empty()) {
        co_return false; // EOF before n bytes
      }
      std::memcpy(static_cast<char *>(dst) + got, chunk.get(), chunk.size());
      got += chunk.size();
    }
    co_return true;
  };

  while (true) {
    uint32_t entryLength = 0;
    bool have = co_await read_exact(&entryLength, sizeof(entryLength));
    if (!have) {
      break; // normal EOF
    }

    // Skip DMA alignment padding.
    //
    // New format (Issue #11 fix): padding is written as
    //   [uint32_t  0]          <-- padding marker (already consumed above)
    //   [uint32_t  skipBytes]  <-- number of additional zero bytes to skip
    //   [skipBytes zero bytes]
    //
    // Backward compatibility with old format (raw zero-filled padding):
    //   Old WAL files wrote only zero bytes for padding. In that case
    //   skipBytes will itself be 0 (since the next 4 bytes are also zero).
    //   We simply loop back and read the next uint32_t, continuing until
    //   we hit a non-zero entryLength or EOF.
    if (entryLength == 0) {
      uint32_t skipBytes = 0;
      if (!(co_await read_exact(&skipBytes, sizeof(skipBytes)))) {
        break; // EOF within padding header
      }

      if (skipBytes > 0) {
        // New-format padding: skip exactly skipBytes additional zero bytes.
        static constexpr uint32_t MAX_SKIP = 16 * 1024 * 1024;
        if (skipBytes > MAX_SKIP) {
          tsdb::wal_log.warn(
              "WAL recovery: unreasonable padding skip count {}, "
              "stopping recovery",
              skipBytes);
          partialEntries++;
          break;
        }
        size_t remaining = skipBytes;
        while (remaining > 0) {
          char discard[4096];
          size_t toRead = std::min(remaining, sizeof(discard));
          if (!(co_await read_exact(discard, toRead))) {
            break; // EOF inside padding body
          }
          remaining -= toRead;
        }
      }
      // For old-format padding (skipBytes == 0), we just consumed 8 zero
      // bytes total (the marker + skipBytes).  Loop back to read the next
      // entryLength; eventually we will reach a non-zero entry or EOF.
      continue;
    }

    // sanity
    static constexpr uint32_t MIN_ENTRY_SIZE = 4;
    static constexpr uint32_t MAX_ENTRY_SIZE = 10 * 1024 * 1024; // 10MB

    if (entryLength < MIN_ENTRY_SIZE || entryLength > MAX_ENTRY_SIZE) {
      tsdb::wal_log.warn(
          "WAL recovery: suspicious entry length {}, stopping recovery",
          entryLength);
      partialEntries++;
      break;
    }

    std::vector<char> entry(entryLength);
    if (!(co_await read_exact(entry.data(), entry.size()))) {
      tsdb::wal_log.warn(
          "WAL recovery: partial tail entry ({} bytes), discarding",
          entryLength);
      partialEntries++;
      break;
    }

    // ---- CRC32 verification ----
    // New format: [crc32 (4 bytes)][payload...]
    // entryLength includes the CRC, so payload = entryLength - 4
    // Old format: [WALType byte][payload...] (no CRC)
    //
    // Detection: if entryLength >= 8, try reading first 4 bytes as CRC and
    // verify against the remaining bytes.  If it matches -> new format.
    // If it doesn't match, check if the first byte is a valid WALType (0-3)
    // which indicates old format.  Otherwise -> genuine corruption.
    const uint8_t *rawEntry = reinterpret_cast<const uint8_t *>(entry.data());
    const uint8_t *payloadPtr = rawEntry;
    size_t payloadSize = entryLength;

    if (entryLength >= 8) {
      uint32_t storedCrc;
      std::memcpy(&storedCrc, rawEntry, sizeof(uint32_t));

      const uint8_t *crcPayload = rawEntry + 4;
      size_t crcPayloadSize = entryLength - 4;
      uint32_t computedCrc = CRC32::compute(crcPayload, crcPayloadSize);

      if (storedCrc == computedCrc) {
        // New format with valid CRC - use payload after CRC
        payloadPtr = crcPayload;
        payloadSize = crcPayloadSize;
        tsdb::wal_log.trace("WAL recovery: CRC32 verified for entry");
      } else {
        // CRC doesn't match - check if this is old format
        uint8_t firstByte = rawEntry[0];
        if (firstByte <= static_cast<uint8_t>(WALType::Close)) {
          // Looks like old format (first byte is a valid WALType)
          tsdb::wal_log.debug(
              "WAL recovery: old format entry detected (no CRC), type={}",
              firstByte);
          // payloadPtr and payloadSize already point to full entry
        } else {
          // Genuine corruption: CRC mismatch and first byte is not a valid type
          tsdb::wal_log.warn(
              "WAL recovery: CRC32 mismatch (stored=0x{:08X}, computed=0x{:08X}), "
              "discarding corrupt entry",
              storedCrc, computedCrc);
          partialEntries++;
          continue;
        }
      }
    }
    // If entryLength < 8, it's too small for new format; treat as old format

    Slice entrySlice(const_cast<uint8_t *>(payloadPtr), payloadSize);
    uint8_t type = entrySlice.read<uint8_t>();

    switch (static_cast<WALType>(type)) {
    case WALType::Write: {
      // Read fixed 16-byte SeriesId128
      std::string seriesIdBytes = entrySlice.readString(16);
      SeriesId128 seriesId = SeriesId128::fromBytes(seriesIdBytes);

      // Read series key string (length-prefixed)
      uint32_t seriesKeyLen = entrySlice.read<uint32_t>();
      std::string seriesKey = entrySlice.readString(seriesKeyLen);

      uint8_t valueType = entrySlice.read<uint8_t>();

      switch (static_cast<WALValueType>(valueType)) {
      case WALValueType::Float: {
        try {
          TSDBInsert<double> insertReq =
              readSeries<double>(entrySlice, seriesKey);
          store->insertMemory(std::move(insertReq));
        } catch (const std::exception &e) {
          tsdb::wal_log.error(
              "WAL recovery: Failed to read float series '{}': {}",
              seriesKey, e.what());
          partialEntries++;
          continue;
        }
      } break;
      case WALValueType::Boolean: {
        try {
          TSDBInsert<bool> insertReq = readSeries<bool>(entrySlice, seriesKey);
          store->insertMemory(std::move(insertReq));
        } catch (const std::exception &e) {
          tsdb::wal_log.error(
              "WAL recovery: Failed to read boolean series '{}': {}",
              seriesKey, e.what());
          partialEntries++;
          continue;
        }
      } break;
      case WALValueType::String: {
        try {
          TSDBInsert<std::string> insertReq =
              readSeries<std::string>(entrySlice, seriesKey);
          store->insertMemory(std::move(insertReq));
        } catch (const std::exception &e) {
          tsdb::wal_log.error(
              "WAL recovery: Failed to read string series '{}': {}",
              seriesKey, e.what());
          partialEntries++;
          continue;
        }
      } break;
      }
      entriesRead++;
    } break;

    case WALType::Close: {
      co_await store->close();
      entriesRead++;
    } break;

    case WALType::DeleteRange: {
      // Read fixed 16-byte SeriesId128
      std::string seriesIdBytes = entrySlice.readString(16);
      SeriesId128 seriesId = SeriesId128::fromBytes(seriesIdBytes);

      uint64_t startTime = entrySlice.read<uint64_t>();
      uint64_t endTime = entrySlice.read<uint64_t>();
      tsdb::wal_log.debug(
          "WAL recovery: DeleteRange for series={}, startTime={}, endTime={}",
          seriesId.toHex(), startTime, endTime);
      store->deleteRange(seriesId, startTime, endTime);
      entriesRead++;
    } break;

    default:
      tsdb::wal_log.warn(
          "WAL recovery: unknown entry type {}, stopping recovery",
          static_cast<int>(type));
      partialEntries++;
      stopRecovery = true;
      break;
    }

    if (stopRecovery) {
      break;
    }
  }

  tsdb::wal_log.info(
      "WAL recovery complete: {} entries read, {} partial entries discarded",
      entriesRead, partialEntries);

  // Close the input stream & file
  try {
    co_await in.close();
  } catch (...) {
  }
  co_await walFile.close();
}

template <class T>
TSDBInsert<T> WALReader::readSeries(Slice &walSlice,
                                    const std::string &seriesKey) {
  TSDBInsert<T> insertReq = TSDBInsert<T>::fromSeriesKey(seriesKey);

  uint32_t timestampsCount = walSlice.read<uint32_t>();
  uint32_t encodedTimestampsSize = walSlice.read<uint32_t>();

  static constexpr uint32_t MAX_TIMESTAMPS = 10000000;           // 10M
  static constexpr uint32_t MAX_ENCODED_SIZE = 50 * 1024 * 1024; // 50MiB

  if (timestampsCount > MAX_TIMESTAMPS) {
    tsdb::wal_log.error("WAL recovery: Invalid timestamps count {} for '{}'",
                        timestampsCount, seriesKey);
    throw std::runtime_error("Invalid timestamps count in WAL entry");
  }
  if (encodedTimestampsSize > MAX_ENCODED_SIZE) {
    tsdb::wal_log.error(
        "WAL recovery: Invalid encoded timestamps size {} for '{}'",
        encodedTimestampsSize, seriesKey);
    throw std::runtime_error("Invalid encoded timestamps size in WAL entry");
  }

  auto timestampsSlice = walSlice.getSlice(encodedTimestampsSize);
  auto [nSkipped, nTimestamps] = IntegerEncoder::decode(
      timestampsSlice, timestampsCount, insertReq.timestamps);

  uint32_t valueByteSize = walSlice.read<uint32_t>();
  if (valueByteSize > MAX_ENCODED_SIZE) {
    tsdb::wal_log.error("WAL recovery: Invalid value size {} for '{}'",
                        valueByteSize, seriesKey);
    throw std::runtime_error("Invalid value byte size in WAL entry");
  }

  if constexpr (std::is_same<T, bool>::value) {
    auto valuesSlice = walSlice.getSlice(valueByteSize);
    BoolEncoder::decode(valuesSlice, nSkipped, nTimestamps, insertReq.values);
  } else if constexpr (std::is_same<T, double>::value) {
    auto valuesSlice = walSlice.getCompressedSlice(valueByteSize);
    FloatDecoder::decode(valuesSlice, nSkipped, nTimestamps, insertReq.values);
  } else if constexpr (std::is_same<T, std::string>::value) {
    auto valuesSlice = walSlice.getSlice(valueByteSize);
    std::vector<std::string> allStrings;
    StringEncoder::decode(valuesSlice, timestampsCount, allStrings);
    insertReq.values.reserve(nTimestamps);
    for (size_t i = nSkipped;
         i < nSkipped + nTimestamps && i < allStrings.size(); i++) {
      insertReq.values.push_back(std::move(allStrings[i]));
    }
  } else {
    throw std::runtime_error("Unsupported data type");
  }

  return insertReq;
}

// Explicit instantiations
template seastar::future<>
WAL::insert<double>(TSDBInsert<double> &insertRequest);
template seastar::future<> WAL::insert<bool>(TSDBInsert<bool> &insertRequest);
template seastar::future<>
WAL::insert<std::string>(TSDBInsert<std::string> &insertRequest);
template size_t
WAL::estimateInsertSize<double>(TSDBInsert<double> &insertRequest);
template size_t WAL::estimateInsertSize<bool>(TSDBInsert<bool> &insertRequest);
template size_t
WAL::estimateInsertSize<std::string>(TSDBInsert<std::string> &insertRequest);
template seastar::future<>
WAL::insertBatch<double>(std::vector<TSDBInsert<double>> &insertRequests);
template seastar::future<>
WAL::insertBatch<bool>(std::vector<TSDBInsert<bool>> &insertRequests);
template seastar::future<> WAL::insertBatch<std::string>(
    std::vector<TSDBInsert<std::string>> &insertRequests);