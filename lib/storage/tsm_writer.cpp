#include "tsm.hpp"
#include "tsm_writer.hpp"
#include "series_id.hpp"
#include "integer_encoder.hpp"
#include "float_encoder.hpp"
#include "bool_encoder_rle.hpp"
#include "string_encoder.hpp"
#include "zigzag.hpp"

#include "logger.hpp"
#include "logging_config.hpp"

#include <algorithm>
#include <limits>
#include <variant>
#include <stdexcept>
#include <system_error>
#include <cstring>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <seastar/core/seastar.hh>
#include <seastar/core/file.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/coroutine.hh>

TSMWriter::TSMWriter(std::string _filename){
  filename = _filename;
  writeHeader();
}

void TSMWriter::writeHeader(){
  std::string magic("TASM");
  buffer.write(magic);
  buffer.write((uint8_t)1); // Version number
}

template <class T>
void TSMWriter::writeSeries(TSMValueType seriesType, const SeriesId128 &seriesId, const std::vector<uint64_t> &timestamps, const std::vector<T> &values){
  // serializes a single series into one or more blocks. After each block, append an index entry
  // TODO: What's the optimum block size??
  TSMIndexEntry indexEntry;
  indexEntry.seriesId = seriesId;
  indexEntry.seriesType = seriesType;

  size_t offset = 0;
  size_t blockCount = 0;

  while(offset < timestamps.size()){
    const size_t end = std::min(timestamps.size(), (size_t)(offset + MaxPointsPerBlock()));
    size_t blockSize = end - offset;
    
    if (blockCount == 0) {
      LOG_INSERT_PATH(timestar::tsm_log, debug, "Creating blocks for series '{}' ({} total points, up to {} per block)",
                      seriesId.toHex(), timestamps.size(), MaxPointsPerBlock());
    }

    //TODO: Avoid the copy here
    LOG_INSERT_PATH(timestar::tsm_log, trace, "Allocating vectors for block {} ({} points)", blockCount, blockSize);
    std::vector<uint64_t> blockTimestamps(timestamps.begin() + offset, timestamps.begin() + end);
    std::vector<T> blockValues(values.begin() + offset, values.begin() + end);
    blockCount++;

    // TODO: Implement bool encoded
    writeBlock(seriesType, seriesId, blockTimestamps, blockValues, indexEntry);

    offset += MaxPointsPerBlock();
  }

  // Phase 4A: Insert into map (keeps sorted automatically)
  indexEntries[seriesId] = std::move(indexEntry);
}

template <class T>
void TSMWriter::writeBlock(TSMValueType seriesType, const SeriesId128 &seriesId, const std::vector<uint64_t> &timestamps, const std::vector<T> &values, TSMIndexEntry &indexEntry){
  size_t blockStartOffset = buffer.size();
  AlignedBuffer encodedTimestamps = IntegerEncoder::encode(timestamps);


  buffer.write((uint8_t)seriesType);  // uint8_t fieldType
  buffer.write((uint32_t)timestamps.size());  // uint32_t timestamp entries count
  buffer.write((uint32_t)encodedTimestamps.size());  // uint32_t timestamp partition length in bytes
  buffer.write(encodedTimestamps);  // uint8_t x N bytes, compressed timestamps

  if constexpr (std::is_same_v<T, double>) {
    CompressedBuffer encodedFloats = FloatEncoder::encode(values);
    buffer.write(encodedFloats);
  } else if constexpr (std::is_same_v<T, bool>) {
    BoolEncoderRLE::encodeInto(values, buffer);
  } else if constexpr (std::is_same_v<T, std::string>) {
    AlignedBuffer encodedStrings = StringEncoder::encode(values);
    buffer.write(encodedStrings);
  } else if constexpr (std::is_same_v<T, int64_t>) {
    auto zigzagged = ZigZag::zigzagEncodeVector(values);
    AlignedBuffer encodedIntegers = IntegerEncoder::encode(zigzagged);
    buffer.write(encodedIntegers);
  } else {
    static_assert(sizeof(T) == 0, "Unsupported TSM value type");
  }

  writeIndexBlock(timestamps, indexEntry, blockStartOffset);
}

// Phase 3.2: Move semantics version for zero-copy writes from batch pool
template <class T>
void TSMWriter::writeSeriesDirect(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<T> &&values) {
  // Zero-copy write - caller transfers ownership of vectors
  // Assumes data is already properly sized (single block or caller handles splitting)

  if (timestamps.size() > MaxPointsPerBlock()) {
    LOG_INSERT_PATH(timestar::tsm_log, debug, "Series has {} points (>{}), writing as multiple blocks with fallback",
                    timestamps.size(), MaxPointsPerBlock());

    // Fallback to copy-based approach for multi-block series
    // (Move-from vectors become lvalues, safe to pass as const&)
    writeSeries(seriesType, seriesId, timestamps, values);
    return;
  }

  // Single block - use move semantics path
  TSMIndexEntry indexEntry;
  indexEntry.seriesId = seriesId;
  indexEntry.seriesType = seriesType;

  LOG_INSERT_PATH(timestar::tsm_log, debug, "Zero-copy write for series '{}' ({} points)",
                  seriesId.toHex(), timestamps.size());

  writeBlockDirect(seriesType, seriesId, std::move(timestamps), std::move(values), indexEntry);
  // Phase 4A: Insert into map (keeps sorted automatically)
  indexEntries[seriesId] = std::move(indexEntry);
}

template <class T>
void TSMWriter::writeBlockDirect(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<T> &&values, TSMIndexEntry &indexEntry) {
  // Phase 3.2: Zero-copy block write
  // Rvalue reference parameters are lvalues once bound, so we can use them normally
  // The caller has transferred ownership - no copy needed on their side

  size_t blockStartOffset = buffer.size();

  // Encoders take const& so they'll read from our moved-to vectors
  AlignedBuffer encodedTimestamps = IntegerEncoder::encode(timestamps);

  buffer.write((uint8_t)seriesType);
  buffer.write((uint32_t)timestamps.size());
  buffer.write((uint32_t)encodedTimestamps.size());
  buffer.write(encodedTimestamps);

  // Encode values based on type
  if constexpr (std::is_same_v<T, double>) {
    CompressedBuffer encodedFloats = FloatEncoder::encode(values);
    buffer.write(encodedFloats);
  } else if constexpr (std::is_same_v<T, bool>) {
    BoolEncoderRLE::encodeInto(values, buffer);
  } else if constexpr (std::is_same_v<T, std::string>) {
    AlignedBuffer encodedStrings = StringEncoder::encode(values);
    buffer.write(encodedStrings);
  } else if constexpr (std::is_same_v<T, int64_t>) {
    auto zigzagged = ZigZag::zigzagEncodeVector(values);
    AlignedBuffer encodedIntegers = IntegerEncoder::encode(zigzagged);
    buffer.write(encodedIntegers);
  } else {
    static_assert(sizeof(T) == 0, "Unsupported TSM value type");
  }

  // Write index block (timestamps still valid as lvalue)
  writeIndexBlock(timestamps, indexEntry, blockStartOffset);
}

void TSMWriter::writeIndexBlock(const std::vector<uint64_t> &timestamps, TSMIndexEntry &indexEntry, size_t blockStartOffset){
  if (timestamps.empty()) {
    return;  // No data to index
  }
  const auto [minTime, maxTime] = std::minmax_element(begin(timestamps), end(timestamps));
  size_t blockSize = buffer.size() - blockStartOffset;

  if (blockSize > std::numeric_limits<uint32_t>::max()) {
    throw std::overflow_error("TSM block size " + std::to_string(blockSize) +
        " exceeds uint32_t maximum (" + std::to_string(std::numeric_limits<uint32_t>::max()) +
        "); block would be truncated in the index");
  }

  TSMIndexBlock indexBlock;
  indexBlock.minTime = *minTime;
  indexBlock.maxTime = *maxTime;
  indexBlock.offset = blockStartOffset;
  indexBlock.size = static_cast<uint32_t>(blockSize);

  indexEntry.indexBlocks.push_back(std::move(indexBlock));
}

// Phase 2: Write compressed block bytes directly (zero-copy transfer)
void TSMWriter::writeCompressedBlock(TSMValueType seriesType, const SeriesId128 &seriesId,
                                     seastar::temporary_buffer<uint8_t> &&compressedData,
                                     uint64_t minTime, uint64_t maxTime) {
  // Record the starting offset for this block
  size_t blockStartOffset = buffer.size();

  // Write the compressed block bytes directly to the buffer
  // The block is already in TSM format (header + compressed timestamps + compressed values)
  buffer.write_bytes(reinterpret_cast<const char*>(compressedData.get()), compressedData.size());

  // Phase 4A: Find or create index entry in map
  auto& indexEntry = indexEntries[seriesId];
  if (indexEntry.seriesId != seriesId) {
    // First time seeing this series, initialize
    indexEntry.seriesId = seriesId;
    indexEntry.seriesType = seriesType;
  }

  // Create index block metadata
  if (compressedData.size() > std::numeric_limits<uint32_t>::max()) {
    throw std::overflow_error("TSM compressed block size " + std::to_string(compressedData.size()) +
        " exceeds uint32_t maximum (" + std::to_string(std::numeric_limits<uint32_t>::max()) +
        "); block would be truncated in the index");
  }

  TSMIndexBlock indexBlock;
  indexBlock.minTime = minTime;
  indexBlock.maxTime = maxTime;
  indexBlock.offset = blockStartOffset;
  indexBlock.size = static_cast<uint32_t>(compressedData.size());

  indexEntry.indexBlocks.push_back(std::move(indexBlock));
}

void TSMWriter::writeIndex(){
  // std::map maintains sorted order automatically
  size_t indexStartOffset = buffer.size();
  LOG_INSERT_PATH(timestar::tsm_log, debug, "Index starts at offset: {} ({:#x}), writing {} index entries",
                  indexStartOffset, indexStartOffset, indexEntries.size());

  // Iterate directly - already sorted by SeriesId128
  for(auto const& [seriesId, indexEntry]: indexEntries){
    // Write SeriesId128 as 16 bytes (no length prefix needed since it's fixed size)
    std::string seriesIdBytes = indexEntry.seriesId.toBytes();
    buffer.write(seriesIdBytes);

    // Block type
    buffer.write((uint8_t)indexEntry.seriesType);  // uint8_t fieldType

    // num of index entries
    if (indexEntry.indexBlocks.size() > std::numeric_limits<uint16_t>::max()) {
      throw std::runtime_error("Too many blocks for series in TSM file: " +
                               std::to_string(indexEntry.indexBlocks.size()));
    }
    buffer.write(static_cast<uint16_t>(indexEntry.indexBlocks.size()));

    // for each block
    for(auto const& block: indexEntry.indexBlocks){
      buffer.write(block.minTime); // minTime
      buffer.write(block.maxTime); // maxTime
      buffer.write(block.offset); // byte offset from start of file
      buffer.write(block.size); // block size
    }
  }

  buffer.write(static_cast<uint64_t>(indexStartOffset));
  LOG_INSERT_PATH(timestar::tsm_log, debug, "Wrote index offset: {} ({:#x}), final buffer size: {}",
                  indexStartOffset, indexStartOffset, buffer.size());
}

// Phase 4A: Parallel index building (placeholder for future enhancement)
void TSMWriter::writeIndexParallel(){
  // For now, just call the regular writeIndex
  // Future: parallelize serialization of index entries
  writeIndex();
}

// fsync the parent directory to ensure a newly-created file's directory
// entry is durable.  Without this, a crash after file fsync but before
// the directory is flushed can lose the file entirely on ext4/XFS.
static void fsyncParentDir(const std::string& filepath) {
  auto slash = filepath.rfind('/');
  std::string dir = (slash != std::string::npos) ? filepath.substr(0, slash) : ".";
  int dirfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
  if (dirfd < 0) return;  // best-effort
  ::fsync(dirfd);
  ::close(dirfd);
}

void TSMWriter::close(){
  LOG_INSERT_PATH(timestar::tsm_log, debug, "Writing file: {}, buffer size: {} ({:#x}), capacity: {}",
                  filename, buffer.size(), buffer.size(), buffer.capacity());

  // Use raw POSIX I/O instead of std::ofstream to avoid the C++ stdio
  // buffering layer (which chunks large writes into 8KB pieces internally).
  // We write the entire buffer in one shot from a contiguous allocation,
  // so a single write() syscall is all that's needed.
  //
  // O_WRONLY | O_CREAT | O_TRUNC: create or truncate, write-only.
  // Mode 0644: owner rw, group/other r.
  int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    throw std::system_error(errno, std::system_category(),
                            "TSMWriter::close: failed to open " + filename);
  }

  // Hint to the kernel that we will write this file sequentially.
  // This allows read-ahead to be disabled and improves write coalescing.
  // POSIX_FADV_SEQUENTIAL is a no-op on failure, so ignore errors.
  ::posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);

  // Write the entire buffer contents in a single large write().
  // For files > 2GB we loop to handle partial writes, but in practice
  // TSM files are well under this limit.
  const char* ptr = reinterpret_cast<const char*>(buffer.data.data());
  size_t remaining = buffer.size();
  while (remaining > 0) {
    ssize_t written = ::write(fd, ptr, remaining);
    if (written < 0) {
      int err = errno;
      ::close(fd);
      throw std::system_error(err, std::system_category(),
                              "TSMWriter::close: write failed for " + filename);
    }
    ptr += written;
    remaining -= static_cast<size_t>(written);
  }

  // fsync on the same fd to ensure durability before returning.
  // Reusing the write fd avoids an extra open() syscall.
  if (::fsync(fd) < 0) {
    int err = errno;
    ::close(fd);
    throw std::system_error(err, std::system_category(),
                            "TSMWriter::close: fsync failed for " + filename);
  }

  if (::close(fd) < 0) {
    timestar::tsm_log.warn("TSMWriter::close: close() failed for {}: {} (errno={})",
                           filename, std::strerror(errno), errno);
  }

  // Ensure the directory entry for this new file is durable.
  fsyncParentDir(filename);

  LOG_INSERT_PATH(timestar::tsm_log, debug, "File written successfully: {}", filename);
}

seastar::future<> TSMWriter::closeDMA(){
  LOG_INSERT_PATH(timestar::tsm_log, debug, "DMA writing file: {}, buffer size: {} ({:#x}), capacity: {}",
                  filename, buffer.size(), buffer.size(), buffer.capacity());

  const size_t dataSize = buffer.size();

  // Open the file for DMA writes (create or truncate, write-only)
  std::string_view filenameView{filename};
  auto file = co_await seastar::open_file_dma(
      filenameView,
      seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate);

  // Ensure we close the file on any exit path (success or exception).
  // GCC 14 does not support co_await in catch blocks, so we use a
  // scope guard pattern: capture any exception, close outside the
  // handler, then rethrow.
  std::exception_ptr writeError;
  try {
    const size_t dmaAlign = file.disk_write_dma_alignment();

    // Pad the data size up to DMA alignment boundary.
    // The file will contain extra zero bytes at the end, but TSM readers
    // use the index offset (last 8 bytes of the *logical* data) to find
    // the index, so they never read past the logical end.  After the
    // write we truncate the file to the exact logical size.
    const size_t paddedSize = (dataSize + dmaAlign - 1) & ~(dmaAlign - 1);

    // The AlignedBuffer's underlying vector uses dma_default_init_allocator
    // which guarantees the base address is aligned to DMA_ALIGNMENT (4096).
    // This satisfies Seastar's memory_dma_alignment requirement, so we can
    // write directly from buffer.data without an intermediate memcpy.
    //
    // We only need to ensure the padding region (beyond dataSize, up to
    // paddedSize) is accessible and zero-filled.  Resize the vector to
    // the padded size -- the allocator default-initializes, so we must
    // explicitly zero the padding bytes for deterministic content.
    const size_t prevCapacity = buffer.data.size();
    if (paddedSize > prevCapacity) {
      buffer.data.resize(paddedSize);
    }
    if (paddedSize > dataSize) {
      std::memset(buffer.data.data() + dataSize, 0, paddedSize - dataSize);
    }

    const char* writePtr = reinterpret_cast<const char*>(buffer.data.data());

    // Write the entire buffer in a single DMA write.  Seastar's
    // dma_write may return a short write count, so loop until all
    // bytes are written.  In practice, for aligned writes up to
    // disk_write_max_length (~1GB), this completes in one call.
    size_t written = 0;
    while (written < paddedSize) {
      size_t n = co_await file.dma_write(written, writePtr + written, paddedSize - written);
      if (n == 0) {
        throw std::runtime_error("TSMWriter::closeDMA: dma_write returned 0 for " + filename);
      }
      written += n;
    }

    // Truncate to exact logical size to remove DMA padding bytes.
    // This ensures the on-disk file is byte-identical to what close() produces.
    if (paddedSize != dataSize) {
      co_await file.truncate(dataSize);
    }

    // Flush for durability (equivalent to fsync)
    co_await file.flush();

  } catch (...) {
    writeError = std::current_exception();
  }

  // Always close the file handle
  co_await file.close();

  if (writeError) {
    std::rethrow_exception(writeError);
  }

  // Ensure the directory entry for this new file is durable.
  fsyncParentDir(filename);

  LOG_INSERT_PATH(timestar::tsm_log, debug, "DMA file written successfully: {}", filename);
}

void TSMWriter::writeAllSeries(TSMWriter& writer, seastar::shared_ptr<MemoryStore> store) {
  for (auto it = store.get()->series.begin(); it != store.get()->series.end(); ++it){
    const auto& seriesKey = it->first;
    auto& memStore = it.value();
    TSMValueType seriesType = (TSMValueType) memStore.index();

    size_t seriesPoints = std::visit([](const auto& s) { return s.timestamps.size(); }, memStore);
    LOG_INSERT_PATH(timestar::tsm_log, debug, "Processing series '{}' with {} points, type={}",
                    seriesKey.toHex(), seriesPoints, static_cast<int>(seriesType));

    SeriesId128 seriesId = seriesKey;

    try {
      switch(seriesType){
        case TSMValueType::Float: {
          auto& series = std::get<InMemorySeries<double>>(memStore);
          series.sort();
          LOG_INSERT_PATH(timestar::tsm_log, trace, "Writing float series '{}' with {} points",
                          seriesKey.toHex(), series.timestamps.size());
          writer.writeSeries(seriesType, seriesId, series.timestamps, series.values);
        }
        break;
        case TSMValueType::Boolean: {
          auto& series = std::get<InMemorySeries<bool>>(memStore);
          series.sort();
          LOG_INSERT_PATH(timestar::tsm_log, trace, "Writing bool series '{}' with {} points",
                          seriesKey.toHex(), series.timestamps.size());
          writer.writeSeries(seriesType, seriesId, series.timestamps, series.values);
        }
        break;
        case TSMValueType::String: {
          auto& series = std::get<InMemorySeries<std::string>>(memStore);
          series.sort();
          LOG_INSERT_PATH(timestar::tsm_log, trace, "Writing string series '{}' with {} points",
                          seriesKey.toHex(), series.timestamps.size());
          writer.writeSeries(seriesType, seriesId, series.timestamps, series.values);
        }
        break;
        case TSMValueType::Integer: {
          auto& series = std::get<InMemorySeries<int64_t>>(memStore);
          series.sort();
          LOG_INSERT_PATH(timestar::tsm_log, trace, "Writing integer series '{}' with {} points",
                          seriesKey.toHex(), series.timestamps.size());
          writer.writeSeries(seriesType, seriesId, series.timestamps, series.values);
        }
        break;
        default:
          timestar::tsm_log.warn("Skipping series '{}' with unknown type {}",
                                  seriesKey.toHex(), static_cast<int>(seriesType));
          break;
      }
    } catch (const std::bad_alloc& e) {
      timestar::tsm_log.error("BAD_ALLOC when processing series '{}' with {} points", seriesKey.toHex(), seriesPoints);
      throw;
    } catch (const std::exception& e) {
      timestar::tsm_log.error("ERROR processing series '{}': {}", seriesKey.toHex(), e.what());
      throw;
    }
  }
}

void TSMWriter::run(seastar::shared_ptr<MemoryStore> store, std::string filename){
  LOG_INSERT_PATH(timestar::tsm_log, info, "Starting TSM write to file: {}, memory store has {} series",
                  filename, store.get()->series.size());

  TSMWriter writer(filename);
  writeAllSeries(writer, store);

  LOG_INSERT_PATH(timestar::tsm_log, debug, "Writing index...");
  writer.writeIndex();
  LOG_INSERT_PATH(timestar::tsm_log, debug, "Closing file...");
  writer.close();
  LOG_INSERT_PATH(timestar::tsm_log, info, "TSM write complete: {}", filename);
}

seastar::future<> TSMWriter::runAsync(seastar::shared_ptr<MemoryStore> store, std::string filename){
  LOG_INSERT_PATH(timestar::tsm_log, info, "Starting async TSM write to file: {}, memory store has {} series",
                  filename, store.get()->series.size());

  TSMWriter writer(filename);
  writeAllSeries(writer, store);

  LOG_INSERT_PATH(timestar::tsm_log, debug, "Writing index...");
  writer.writeIndex();
  LOG_INSERT_PATH(timestar::tsm_log, debug, "Closing file via DMA...");
  co_await writer.closeDMA();
  LOG_INSERT_PATH(timestar::tsm_log, info, "Async TSM write complete: {}", filename);
}

// Template instantiations
template void TSMWriter::writeSeries<double>(TSMValueType seriesType, const SeriesId128 &seriesId, const std::vector<uint64_t> &timestamps, const std::vector<double> &values);
template void TSMWriter::writeSeries<bool>(TSMValueType seriesType, const SeriesId128 &seriesId, const std::vector<uint64_t> &timestamps, const std::vector<bool> &values);
template void TSMWriter::writeSeries<std::string>(TSMValueType seriesType, const SeriesId128 &seriesId, const std::vector<uint64_t> &timestamps, const std::vector<std::string> &values);
template void TSMWriter::writeSeries<int64_t>(TSMValueType seriesType, const SeriesId128 &seriesId, const std::vector<uint64_t> &timestamps, const std::vector<int64_t> &values);

// Phase 3.2: Template instantiations for move semantics versions
template void TSMWriter::writeSeriesDirect<double>(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<double> &&values);
template void TSMWriter::writeSeriesDirect<bool>(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<bool> &&values);
template void TSMWriter::writeSeriesDirect<std::string>(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<std::string> &&values);
template void TSMWriter::writeSeriesDirect<int64_t>(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<int64_t> &&values);

template void TSMWriter::writeBlockDirect<double>(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<double> &&values, TSMIndexEntry &indexEntry);
template void TSMWriter::writeBlockDirect<bool>(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<bool> &&values, TSMIndexEntry &indexEntry);
template void TSMWriter::writeBlockDirect<std::string>(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<std::string> &&values, TSMIndexEntry &indexEntry);
template void TSMWriter::writeBlockDirect<int64_t>(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<int64_t> &&values, TSMIndexEntry &indexEntry);
