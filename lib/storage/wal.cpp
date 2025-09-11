// wal.cpp — stream-based, unaligned I/O version (fixed build errors)

#include "wal.hpp"

#include "aligned_buffer.hpp"
#include "tsm.hpp"
#include "integer_encoder.hpp"
#include "float_encoder.hpp"
#include "bool_encoder.hpp"
#include "string_encoder.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include "series_id.hpp"

#include <filesystem>
#include <chrono>
#include <optional>
#include <string>
#include <vector>

#include <seastar/core/reactor.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>

namespace fs = std::filesystem;

// ------------------------ WAL ------------------------

WAL::WAL(unsigned int _sequenceNumber)
    : sequenceNumber(_sequenceNumber) {}

WAL::~WAL() {
  // Note: Can't do async ops in destructor. Warn if anything left buffered.
  if (bufferPos > 0) {
    tsdb::wal_log.warn("WAL destructor called with unflushed data ({} bytes)", bufferPos);
  }
}

seastar::future<> WAL::init(MemoryStore* /*store*/, bool isRecovery) {
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
                         "Truncating to start fresh.", filename);
    }
    openFlags = seastar::open_flags::rw | seastar::open_flags::create | seastar::open_flags::truncate;
    tsdb::wal_log.debug("Creating fresh WAL {}", filename);
  }

  walFile = co_await seastar::open_file_dma(filenameView, openFlags);

  if (!walFile) {
    tsdb::wal_log.error("Failed to open WAL file: {}", filename);
    co_return;
  } else {
    tsdb::wal_log.debug("WAL file opened: {}", filename);
  }

  // Get current file size
  filePos = co_await walFile.size();
  currentSize = filePos;

  
  if (!isRecovery && currentSize > 0) {
    tsdb::wal_log.error("Fresh WAL {} has non-zero size {} after truncate - this is unexpected", 
                        filename, currentSize.load());
  }

  // Build buffered output stream positioned at EOF (append)
  // Use the (file, offset) overload in this Seastar version.
  auto s = co_await seastar::make_file_output_stream(walFile);
  out.emplace(std::move(s));

  tsdb::wal_log.debug("WAL stream init: pos={}", filePos);
}

std::string WAL::sequenceNumberToFilename(unsigned int sequenceNumber) {
  std::string path = "shard_" + std::to_string(seastar::this_shard_id()) + "/";
  std::string sequenceNumStr = std::to_string(sequenceNumber);
  std::string filename =
      path + std::string(10 - sequenceNumStr.length(), '0').append(sequenceNumStr).append(".wal");
  return filename;
}

seastar::future<> WAL::finalFlush() {
  if (!out) co_return;
  try {
    co_await out->flush();   // drains buffers and fsyncs the sink
    tsdb::wal_log.debug("WAL final flush completed, filePos={}", filePos);
  } catch (const std::exception& e) {
    tsdb::wal_log.error("WAL final flush error: {}", e.what());
  }
}

seastar::future<> WAL::close() {
  // Prevent double-close
  if (_closed || !walFile) {
    tsdb::wal_log.debug("WAL seq={} already closed or invalid file", sequenceNumber);
    co_return;
  }
  
  _closed = true;
  tsdb::wal_log.debug("WAL seq={} starting close", sequenceNumber);

  // Skip Close marker and stream operations during shutdown to avoid hangs
  if (out) {
    tsdb::wal_log.debug("WAL seq={} resetting output stream without close", sequenceNumber);
    // Just reset - let the file close handle cleanup
    out.reset();
  }

  // Close the underlying file
  tsdb::wal_log.debug("WAL seq={} closing file", sequenceNumber);
  try {
    co_await walFile.close();
    tsdb::wal_log.debug("WAL seq={} file close completed", sequenceNumber);
  } catch (const std::exception& e) {
    tsdb::wal_log.error("WAL seq={} file close error: {}", sequenceNumber, e.what());
    // Don't rethrow during shutdown
  }
  
  tsdb::wal_log.debug("WAL seq={} closed successfully", sequenceNumber);
}

seastar::future<unsigned long> WAL::size() {
  auto s = co_await walFile.size();
  co_return static_cast<unsigned long>(s);
}

seastar::future<> WAL::remove() {
  std::string filename = WAL::sequenceNumberToFilename(sequenceNumber);
  return seastar::remove_file(filename);
}

// Flush pending buffered bytes in the output stream
seastar::future<> WAL::flushBlock() {
  if (!out) co_return;
  try {
    auto startTime = std::chrono::steady_clock::now();

    co_await out->flush();   // single flush; avoid double-fsync

    auto endTime = std::chrono::steady_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
    LOG_INSERT_PATH(tsdb::wal_log, debug,
                    "WAL stream flush: pos={}, took={}ms",
                    filePos, ms);
    if (ms > 100) {
      tsdb::wal_log.warn("WAL flush took {}ms - potential I/O bottleneck", ms);
    }
  } catch (const std::exception& e) {
    tsdb::wal_log.error("WAL flush error: {}", e.what());
  }
}

template <class T>
size_t WAL::estimateInsertSize(TSDBInsert<T>& insertRequest) {
  size_t estimatedSize = 0;

  estimatedSize += 4;  // length
  estimatedSize += 1;  // WAL type
  estimatedSize += 16; // SeriesId128 (fixed 16 bytes)
  estimatedSize += 1;  // value type
  estimatedSize += 4;  // # of timestamps

  AlignedBuffer encodedTimestamps = IntegerEncoder::encode(insertRequest.timestamps);
  estimatedSize += 4 + encodedTimestamps.size();

  estimatedSize += 4; // values byte-size
  if constexpr (std::is_same<T, double>::value) {
    CompressedBuffer encodedFloats = FloatEncoder::encode(insertRequest.values);
    estimatedSize += encodedFloats.size();
  } else if constexpr (std::is_same<T, bool>::value) {
    AlignedBuffer encodedBools = BoolEncoder::encode(insertRequest.values);
    estimatedSize += encodedBools.size();
  } else if constexpr (std::is_same<T, std::string>::value) {
    AlignedBuffer encodedStrings = StringEncoder::encode(insertRequest.values);
    estimatedSize += encodedStrings.size();
  }

  return estimatedSize;
}

template <class T>
seastar::future<> WAL::insert(TSDBInsert<T>& insertRequest) {
  AlignedBuffer buffer;

  // Reserve space for entry length (4 bytes)
  buffer.write((uint32_t)0);
  size_t lengthPos = 0;

  buffer.write((uint8_t)WALType::Write);

  // Store SeriesId128 (fixed 16 bytes)
  SeriesId128 seriesId = insertRequest.seriesId128();
  std::string seriesIdBytes = seriesId.toBytes();
  buffer.write(seriesIdBytes);

  // Value type
  buffer.write((uint8_t)TSM::getValueType<T>());

  // Num of timestamps
  buffer.write((uint32_t)insertRequest.timestamps.size());

  // Encoded timestamps
  {
    AlignedBuffer encodedTimestamps = IntegerEncoder::encode(insertRequest.timestamps);
    buffer.write((uint32_t)encodedTimestamps.size());
    buffer.write(encodedTimestamps);
  }

  // Encoded values
  if constexpr (std::is_same<T, double>::value) {
    CompressedBuffer encodedFloats = FloatEncoder::encode(insertRequest.values);
    buffer.write((uint32_t)encodedFloats.size());
    buffer.write(encodedFloats);
  } else if constexpr (std::is_same<T, bool>::value) {
    AlignedBuffer encodedBools = BoolEncoder::encode(insertRequest.values);
    buffer.write((uint32_t)encodedBools.size());
    buffer.write(encodedBools);
  } else if constexpr (std::is_same<T, std::string>::value) {
    AlignedBuffer encodedStrings = StringEncoder::encode(insertRequest.values);
    buffer.write((uint32_t)encodedStrings.size());
    buffer.write(encodedStrings);
  } else {
    throw std::runtime_error("Unsupported data type");
  }

  const size_t dataSize = buffer.size();

  // Update entry length at the beginning (excluding the length field itself)
  const uint32_t entryLength = static_cast<uint32_t>(dataSize - sizeof(uint32_t));
  memcpy(buffer.data.data() + lengthPos, &entryLength, sizeof(uint32_t));

  LOG_INSERT_PATH(tsdb::wal_log, debug,
                  "WAL::insert - dataSize={}, entryLength={}, currentSize={}",
                  dataSize, entryLength, currentSize.load());

  // Respect the 16MiB WAL limit BEFORE writing
  const size_t projectedSize = currentSize.load() + dataSize;
  if (projectedSize > MAX_WAL_SIZE) {
    tsdb::wal_log.debug(
        "WAL::insert - Would exceed 16MB limit (current={}, dataSize={}, projected={}), signaling rollover needed",
        currentSize.load(), dataSize, projectedSize);
    throw std::runtime_error("WAL rollover needed");
  }

  // Stream write (unaligned)
  try {
    if (out) {
      co_await out->write(reinterpret_cast<const char*>(buffer.data.data()), dataSize);
    } else {
      throw std::runtime_error("WAL output stream is null");
    }
    // Update positions and size
    filePos += dataSize;
    currentSize.fetch_add(dataSize);

    // Only flush immediately if configured for immediate mode
    if (requiresImmediateFlush) {
      co_await out->flush();
    }
  } catch (const std::exception& e) {
    tsdb::wal_log.error("WAL::insert write failed: {}", e.what());
    throw;
  }
}

template <class T>
seastar::future<> WAL::insertBatch(std::vector<TSDBInsert<T>>& insertRequests) {
  if (insertRequests.empty()) {
    co_return;
  }
  if (insertRequests.size() == 1) {
    co_await insert(insertRequests[0]);
    co_return;
  }

  // Prepare all buffers first
  std::vector<AlignedBuffer> buffers;
  buffers.reserve(insertRequests.size());
  size_t totalSize = 0;

  for (auto& insertRequest : insertRequests) {
    AlignedBuffer buf;
    buf.write((uint32_t)0);  // placeholder for length

    buf.write((uint8_t)WALType::Write);

    // Store SeriesId128 (fixed 16 bytes)
    SeriesId128 seriesId = insertRequest.seriesId128();
    std::string seriesIdBytes = seriesId.toBytes();
    buf.write(seriesIdBytes);

    buf.write((uint8_t)TSM::getValueType<T>());
    buf.write((uint32_t)insertRequest.timestamps.size());

    {
      AlignedBuffer encodedTimestamps = IntegerEncoder::encode(insertRequest.timestamps);
      buf.write((uint32_t)encodedTimestamps.size());
      buf.write(encodedTimestamps);
    }

    if constexpr (std::is_same<T, double>::value) {
      CompressedBuffer encodedFloats = FloatEncoder::encode(insertRequest.values);
      buf.write((uint32_t)encodedFloats.size());
      buf.write(encodedFloats);
    } else if constexpr (std::is_same<T, bool>::value) {
      AlignedBuffer encodedBools = BoolEncoder::encode(insertRequest.values);
      buf.write((uint32_t)encodedBools.size());
      buf.write(encodedBools);
    } else if constexpr (std::is_same<T, std::string>::value) {
      AlignedBuffer encodedStrings = StringEncoder::encode(insertRequest.values);
      buf.write((uint32_t)encodedStrings.size());
      buf.write(encodedStrings);
    }

    uint32_t entryLength = static_cast<uint32_t>(buf.size() - sizeof(uint32_t));
    memcpy(buf.data.data(), &entryLength, sizeof(uint32_t));

    totalSize += buf.size();
    buffers.push_back(std::move(buf));
  }

  LOG_INSERT_PATH(tsdb::wal_log, debug,
                  "WAL::insertBatch - {} entries, total size={}, currentSize={}",
                  buffers.size(), totalSize, currentSize.load());

  // WAL limit check (pre-flight)
  const size_t projected = currentSize.load() + totalSize;
  if (projected > MAX_WAL_SIZE) {
    tsdb::wal_log.debug(
        "WAL::insertBatch - Would exceed 16MB limit (current={}, total={}, projected={}), signaling rollover",
        currentSize.load(), totalSize, projected);
    throw std::runtime_error("WAL rollover needed");
  }

  try {
    size_t written = 0;
    if (out) {
      // Write each encoded buffer; output_stream will coalesce internally.
      for (auto& b : buffers) {
        co_await out->write(reinterpret_cast<const char*>(b.data.data()), b.size());
        written += b.size();
      }
    }
    filePos += written;
    currentSize.fetch_add(written);

    // Only flush immediately if configured for immediate mode
    if (requiresImmediateFlush) {
      co_await out->flush();
    }
  } catch (const std::exception& e) {
    tsdb::wal_log.error("WAL::insertBatch write failed: {}", e.what());
    throw;
  }
}

seastar::future<> WAL::deleteRange(const SeriesId128& seriesId,
                                   uint64_t startTime,
                                   uint64_t endTime) {
  AlignedBuffer buffer;

  // Reserve space for entry length (4 bytes)
  buffer.write((uint32_t)0);
  size_t lengthPos = 0;

  // Type
  buffer.write((uint8_t)WALType::DeleteRange);

  // Series ID (fixed 16 bytes)
  std::string seriesIdBytes = seriesId.toBytes();
  buffer.write(seriesIdBytes);

  // Time range
  buffer.write(startTime);
  buffer.write(endTime);

  // Update entry length
  uint32_t entryLength = static_cast<uint32_t>(buffer.size() - 4);
  memcpy(buffer.data.data() + lengthPos, &entryLength, 4);

  const size_t n = buffer.size();

  try {
    if (out) {
      co_await out->write(reinterpret_cast<const char*>(buffer.data.data()), n);
    }
    filePos += n;
    currentSize.fetch_add(n);
    
    // Only flush immediately if configured for immediate mode
    if (requiresImmediateFlush) {
      co_await out->flush();
    }
  } catch (const std::exception& e) {
    tsdb::wal_log.error("WAL::deleteRange write failed: {}", e.what());
    throw;
  }

  LOG_INSERT_PATH(tsdb::wal_log, debug,
                  "WAL deleteRange written: series={}, startTime={}, endTime={}, size={} bytes",
                  seriesId.toHex(), startTime, endTime, n);
}

void WAL::remove(unsigned int sequenceNumber) {
  std::string filename = WAL::sequenceNumberToFilename(sequenceNumber);

  try {
    if (std::filesystem::remove(filename))
      tsdb::wal_log.debug("WAL file {} deleted", filename);
    else
      tsdb::wal_log.debug("WAL file {} not found", filename);
  } catch (const std::filesystem::filesystem_error& err) {
    tsdb::wal_log.error("Filesystem error removing WAL: {}", err.what());
  }
}

// ------------------------ WALReader ------------------------

WALReader::WALReader(std::string _filename)
    : filename(std::move(_filename)) {}

seastar::future<> WALReader::readAll(MemoryStore* store) {
  std::string_view filenameView{filename};
  walFile = co_await seastar::open_file_dma(filenameView, seastar::open_flags::ro);

  if (!walFile) {
    tsdb::wal_log.error("Failed to open WAL file: {}", filename);
    throw std::runtime_error("Failed opening WAL file");
  }

  length = co_await walFile.size();
  if (length == 0) {
    tsdb::wal_log.info("WAL recovery complete: 0 entries read, 0 partial entries discarded");
    co_await walFile.close();
    co_return;
  }

  tsdb::wal_log.info("WAL recovery starting for file {} with size {} bytes", filename, length);

  size_t entriesRead = 0;
  size_t partialEntries = 0;

  // Stream the file; no aligned over-reads
  auto in = seastar::make_file_input_stream(walFile);

  // read_exact helper using read_up_to (no trim_front needed)
  auto read_exact = [&in](void* dst, size_t n) -> seastar::future<bool> {
    size_t got = 0;
    while (got < n) {
      auto chunk = co_await in.read_up_to(n - got);
      if (chunk.empty()) {
        co_return false; // EOF before n bytes
      }
      std::memcpy(static_cast<char*>(dst) + got, chunk.get(), chunk.size());
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

    // sanity
    static constexpr uint32_t MIN_ENTRY_SIZE = 4;
    static constexpr uint32_t MAX_ENTRY_SIZE = 10 * 1024 * 1024; // 10MB

    if (entryLength < MIN_ENTRY_SIZE || entryLength > MAX_ENTRY_SIZE) {
      tsdb::wal_log.warn("WAL recovery: suspicious entry length {}, stopping recovery", entryLength);
      partialEntries++;
      break;
    }

    std::vector<char> entry(entryLength);
    if (!(co_await read_exact(entry.data(), entry.size()))) {
      tsdb::wal_log.warn("WAL recovery: partial tail entry ({} bytes), discarding", entryLength);
      partialEntries++;
      break;
    }

    Slice entrySlice(reinterpret_cast<uint8_t*>(entry.data()), entry.size());
    uint8_t type = entrySlice.read<uint8_t>();

    switch (static_cast<WALType>(type)) {
      case WALType::Write: {
        // Read fixed 16-byte SeriesId128
        std::string seriesIdBytes = entrySlice.readString(16);
        SeriesId128 seriesId = SeriesId128::fromBytes(seriesIdBytes);
        
        uint8_t valueType = entrySlice.read<uint8_t>();

        switch (static_cast<WALValueType>(valueType)) {
          case WALValueType::Float: {
            try {
              TSDBInsert<double> insertReq = readSeries<double>(entrySlice, seriesId);
              store->insertMemory(insertReq);
            } catch (const std::exception& e) {
              tsdb::wal_log.error("WAL recovery: Failed to read float series '{}': {}", seriesId.toHex(), e.what());
              partialEntries++;
              continue;
            }
          } break;
          case WALValueType::Boolean: {
            try {
              TSDBInsert<bool> insertReq = readSeries<bool>(entrySlice, seriesId);
              store->insertMemory(insertReq);
            } catch (const std::exception& e) {
              tsdb::wal_log.error("WAL recovery: Failed to read boolean series '{}': {}", seriesId.toHex(), e.what());
              partialEntries++;
              continue;
            }
          } break;
          case WALValueType::String: {
            try {
              TSDBInsert<std::string> insertReq = readSeries<std::string>(entrySlice, seriesId);
              store->insertMemory(insertReq);
            } catch (const std::exception& e) {
              tsdb::wal_log.error("WAL recovery: Failed to read string series '{}': {}", seriesId.toHex(), e.what());
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
        tsdb::wal_log.debug("WAL recovery: DeleteRange for series={}, startTime={}, endTime={}",
                            seriesId.toHex(), startTime, endTime);
        store->deleteRange(seriesId, startTime, endTime);
        entriesRead++;
      } break;

      default:
        tsdb::wal_log.warn("WAL recovery: unknown entry type {}, stopping recovery",
                           static_cast<int>(type));
        partialEntries++;
        goto recovery_complete;
    }
  }

recovery_complete:
  tsdb::wal_log.info("WAL recovery complete: {} entries read, {} partial entries discarded",
                     entriesRead, partialEntries);

  // Close the input stream & file
  try { co_await in.close(); } catch (...) {}
  co_await walFile.close();
}

template <class T>
TSDBInsert<T> WALReader::readSeries(Slice& walSlice, const SeriesId128& seriesId) {
  std::string seriesKey = seriesId.toHex();
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
    tsdb::wal_log.error("WAL recovery: Invalid encoded timestamps size {} for '{}'",
                        encodedTimestampsSize, seriesKey);
    throw std::runtime_error("Invalid encoded timestamps size in WAL entry");
  }

  auto timestampsSlice = walSlice.getSlice(encodedTimestampsSize);
  auto [nSkipped, nTimestamps] = IntegerEncoder::decode(timestampsSlice, timestampsCount, insertReq.timestamps);

  uint32_t valueByteSize = walSlice.read<uint32_t>();
  if (valueByteSize > MAX_ENCODED_SIZE) {
    tsdb::wal_log.error("WAL recovery: Invalid value size {} for '{}'", valueByteSize, seriesKey);
    throw std::runtime_error("Invalid value byte size in WAL entry");
  }

  if constexpr (std::is_same<T, bool>::value) {
    auto valuesSlice = walSlice.getSlice(valueByteSize);
    BoolEncoder::decode(valuesSlice, nSkipped, nTimestamps, insertReq.values);
  } else if constexpr (std::is_same<T, double>::value) {
    auto valuesSlice = walSlice.getCompressedSlice(valueByteSize);
    FloatEncoder::decode(valuesSlice, nSkipped, nTimestamps, insertReq.values);
  } else if constexpr (std::is_same<T, std::string>::value) {
    auto valuesSlice = walSlice.getSlice(valueByteSize);
    std::vector<std::string> allStrings;
    StringEncoder::decode(valuesSlice, timestampsCount, allStrings);
    insertReq.values.reserve(nTimestamps);
    for (size_t i = nSkipped; i < nSkipped + nTimestamps && i < allStrings.size(); i++) {
      insertReq.values.push_back(std::move(allStrings[i]));
    }
  } else {
    throw std::runtime_error("Unsupported data type");
  }

  return insertReq;
}

// Explicit instantiations
template seastar::future<> WAL::insert<double>(TSDBInsert<double>& insertRequest);
template seastar::future<> WAL::insert<bool>(TSDBInsert<bool>& insertRequest);
template seastar::future<> WAL::insert<std::string>(TSDBInsert<std::string>& insertRequest);
template size_t WAL::estimateInsertSize<double>(TSDBInsert<double>& insertRequest);
template size_t WAL::estimateInsertSize<bool>(TSDBInsert<bool>& insertRequest);
template size_t WAL::estimateInsertSize<std::string>(TSDBInsert<std::string>& insertRequest);
template seastar::future<> WAL::insertBatch<double>(std::vector<TSDBInsert<double>>& insertRequests);
template seastar::future<> WAL::insertBatch<bool>(std::vector<TSDBInsert<bool>>& insertRequests);
template seastar::future<> WAL::insertBatch<std::string>(std::vector<TSDBInsert<std::string>>& insertRequests);