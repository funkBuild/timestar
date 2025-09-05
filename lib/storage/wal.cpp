#include "wal.hpp"
#include "aligned_buffer.hpp"
#include "tsm.hpp"
#include "integer_encoder.hpp"
#include "float_encoder.hpp"
#include "bool_encoder.hpp"
#include "string_encoder.hpp"
#include "logger.hpp"
#include "logging_config.hpp"

#include <filesystem>
#include <seastar/core/reactor.hh>
#include <seastar/core/timer.hh>

namespace fs = std::filesystem;

// TODO: WAL directory should be set in config
WAL::WAL(unsigned int _sequenceNumber) : sequenceNumber(_sequenceNumber) {};

WAL::~WAL() {
  // Note: Can't do async operations in destructor
  // Ensure finalFlush() is called before destruction
  if (bufferPos > 0) {
    tsdb::wal_log.warn("WAL destructor called with unflushed data ({} bytes)", bufferPos);
  }
}

seastar::future<> WAL::init(MemoryStore *store){
  std::string filename = sequenceNumberToFilename(sequenceNumber);

  if(fs::exists(filename)){
    tsdb::wal_log.debug("WAL file exists: {}", filename);
    // Initialize current size from existing file
    currentSize = fs::file_size(filename);
  }

  std::string_view filenameView{ filename }; // TODO: string_view as function parameter instead of string

  walFile = co_await seastar::open_file_dma(filenameView, seastar::open_flags::rw | seastar::open_flags::create);

  if(!walFile)
    tsdb::wal_log.error("Failed to open WAL file: {}", filename);
  else
    tsdb::wal_log.debug("WAL file opened: {}", filename);

  // Initialize aligned write buffer
  writeBuffer = seastar::temporary_buffer<char>::aligned(WAL_ALIGNMENT, WAL_BLOCK_SIZE);
  memset(writeBuffer.get_write(), 0, WAL_BLOCK_SIZE);
  
  // If recovering existing file, align file position
  auto file_size = co_await walFile.size();
  if (file_size > 0) {
    filePos = align_up(file_size, WAL_ALIGNMENT);
    if (filePos != file_size) {
      co_await walFile.truncate(filePos);  // Ensure file is aligned
      tsdb::wal_log.debug("Aligned WAL file from {} to {} bytes", file_size, filePos);
    }
    currentSize = filePos;
  }
  
  tsdb::wal_log.debug("WAL initialized with buffer size: {}, alignment: {}", WAL_BLOCK_SIZE, WAL_ALIGNMENT);
  
  // Set up periodic flush timer for batched writes
  flushTimer.set_callback([this] {
    if (bufferPos > 0) {
      // Schedule async flush
      (void)flushBlock();
    }
  });
  flushTimer.arm_periodic(FLUSH_INTERVAL);
  tsdb::wal_log.debug("WAL periodic flush timer armed with {}ms interval", FLUSH_INTERVAL.count());
}

std::string WAL::sequenceNumberToFilename(unsigned int sequenceNumber){
  std::string path = "shard_" + std::to_string(seastar::this_shard_id()) + "/";
  std::string sequenceNumStr = std::to_string(sequenceNumber);
  std::string filename = path + std::string(10 - sequenceNumStr.length(), '0').append(sequenceNumStr).append(".wal");

  return std::move(filename);
}

seastar::future<> WAL::finalFlush() {
  if (bufferPos > 0) {
    co_await flushBlock();
    tsdb::wal_log.debug("WAL final flush completed, {} bytes written", filePos);
  }
}

seastar::future<> WAL::close(){
  if(!walFile)
    co_return;
  
  // Cancel the flush timer
  flushTimer.cancel();
  
  // Write close marker to buffer
  AlignedBuffer buffer;
  buffer.write((uint32_t)1);  // Entry length (just the type byte)
  buffer.write((uint8_t)WALType::Close);
  
  size_t dataSize = buffer.size();
  
  // Check if data fits in current block
  if (bufferPos + dataSize > WAL_BLOCK_SIZE) {
    co_await flushBlock();
  }
  
  // Copy close marker to write buffer
  memcpy(writeBuffer.get_write() + bufferPos, buffer.data.data(), dataSize);
  bufferPos += dataSize;
  
  // Flush final block
  co_await flushBlock();
  
  // Close the file
  co_await walFile.close();
  
  tsdb::wal_log.debug("WAL closed, final position: {}", filePos);
}

seastar::future<unsigned long> WAL::size(){
  return walFile.size();
};

seastar::future<> WAL::remove(){
  std::string filename = WAL::sequenceNumberToFilename(sequenceNumber);
  return seastar::remove_file(filename);
};


seastar::future<> WAL::flushBlock() {
  if (bufferPos == 0) {
    co_return;  // Nothing to flush
  }
  
  // Save original buffer position for logging
  size_t originalPos = bufferPos;
  
  // Pad to alignment boundary
  size_t alignedSize = align_up(bufferPos, WAL_ALIGNMENT);
  
  // Write aligned block
  co_await walFile.dma_write(filePos, writeBuffer.get(), alignedSize);
  
  // Update positions
  filePos += alignedSize;
  currentSize = filePos;
  
  // Reset buffer position
  bufferPos = 0;
  
  // Clear buffer for next use
  memset(writeBuffer.get_write(), 0, WAL_BLOCK_SIZE);
  
  LOG_INSERT_PATH(tsdb::wal_log, debug, "Flushed WAL block: {} bytes (aligned from {}), filePos now {}", 
                      alignedSize, originalPos, filePos);
}

template <class T>
size_t WAL::estimateInsertSize(TSDBInsert<T> &insertRequest){
  // Calculate the actual size that will be written to WAL (uncompressed)
  // We need to actually encode the data to get the exact size
  size_t estimatedSize = 0;
  
  // Entry length prefix (4 bytes)
  estimatedSize += 4;
  
  // WAL type (1 byte)
  estimatedSize += 1;
  
  // Series ID length (2 bytes) + series ID string
  std::string seriesId = insertRequest.seriesKey();
  estimatedSize += 2 + seriesId.length();
  
  // Value type (1 byte)
  estimatedSize += 1;
  
  // Number of timestamps (4 bytes)
  estimatedSize += 4;
  
  // Encoded timestamps - need actual encoded size
  AlignedBuffer encodedTimestamps = IntegerEncoder::encode(insertRequest.timestamps);
  estimatedSize += 4 + encodedTimestamps.size();  // Size prefix (4 bytes) + actual data
  
  // Encoded values size (4 bytes) + actual encoded size
  estimatedSize += 4;
  if constexpr (std::is_same<T, double>::value){
    CompressedBuffer encodedFloats = FloatEncoder::encode(insertRequest.values);
    estimatedSize += encodedFloats.size();
  } else if constexpr (std::is_same<T, bool>::value){
    AlignedBuffer encodedBools = BoolEncoder::encode(insertRequest.values);
    estimatedSize += encodedBools.size();
  } else if constexpr (std::is_same<T, std::string>::value){
    AlignedBuffer encodedStrings = StringEncoder::encode(insertRequest.values);
    estimatedSize += encodedStrings.size();
  }
  
  return estimatedSize;
}

template <class T>
seastar::future<> WAL::insert(TSDBInsert<T> &insertRequest){
  AlignedBuffer buffer;
  LOG_INSERT_PATH(tsdb::wal_log, trace, "WAL write buffer size: {}", buffer.size());
  
  // Reserve space for entry length (4 bytes)
  buffer.write((uint32_t)0);  // Will be updated with actual length
  
  size_t lengthPos = 0;  // Position where we wrote the length
  
  buffer.write((uint8_t)WALType::Write);

  std::string seriesId = insertRequest.seriesKey();
  uint16_t seriesIdLength = seriesId.length();
  buffer.write(seriesIdLength);
  buffer.write(seriesId);
  LOG_INSERT_PATH(tsdb::wal_log, trace, "WAL write buffer size: {}", buffer.size());

  // Value type
  buffer.write((uint8_t) TSM::getValueType<T>());

  // Num of timestamps
  buffer.write((uint32_t)insertRequest.timestamps.size());

  AlignedBuffer encodedTimestamps = IntegerEncoder::encode(insertRequest.timestamps);
  buffer.write((uint32_t)encodedTimestamps.size());
  buffer.write(encodedTimestamps);

  if constexpr (std::is_same<T, double>::value){
    CompressedBuffer encodedFloats = FloatEncoder::encode(insertRequest.values);

    buffer.write((uint32_t)encodedFloats.size());
    buffer.write(encodedFloats);  // uint8_t x N bytes, compressed values

  } else if constexpr (std::is_same<T, bool>::value){
    AlignedBuffer encodedBools = BoolEncoder::encode(insertRequest.values);

    buffer.write((uint32_t)encodedBools.size());
    buffer.write(encodedBools);  // uint8_t x N bytes, compressed values

  } else if constexpr (std::is_same<T, std::string>::value){
    AlignedBuffer encodedStrings = StringEncoder::encode(insertRequest.values);

    buffer.write((uint32_t)encodedStrings.size());
    buffer.write(encodedStrings);  // uint8_t x N bytes, compressed values

  } else {
    throw std::runtime_error("Unsupported data type");
  }

  size_t dataSize = buffer.size();
  
  // Update the entry length at the beginning (excluding the length field itself)
  uint32_t entryLength = dataSize - sizeof(uint32_t);
  memcpy(buffer.data.data() + lengthPos, &entryLength, sizeof(uint32_t));
  
  // Debug: Log write size
  LOG_INSERT_PATH(tsdb::wal_log, debug, "WAL::insert - dataSize={}, entryLength={}", dataSize, entryLength);
  
  // Check if data fits in current block
  if (bufferPos + dataSize > WAL_BLOCK_SIZE) {
    // Flush current block before writing new data
    co_await flushBlock();
  }
  
  // Check if data is larger than a single block (shouldn't happen in practice)
  if (dataSize > WAL_BLOCK_SIZE) {
    throw std::runtime_error("WAL entry too large for single block");
  }
  
  // Copy data to write buffer
  memcpy(writeBuffer.get_write() + bufferPos, buffer.data.data(), dataSize);
  bufferPos += dataSize;
  
  // Update current size
  currentSize.fetch_add(dataSize);
  
  // Flush strategies:
  // 1. If buffer is nearly full (>90%), flush immediately
  // 2. If immediate flush is required (for critical data), flush immediately
  // 3. Otherwise, let the periodic timer handle it for batching
  
  bool shouldFlush = requiresImmediateFlush || 
                     (bufferPos > WAL_BLOCK_SIZE * 0.9);
  
  if (shouldFlush) {
    co_await flushBlock();
  }
  // Otherwise, the periodic timer will flush within FLUSH_INTERVAL ms
};

template <class T>
seastar::future<> WAL::insertBatch(std::vector<TSDBInsert<T>>& insertRequests) {
  if (insertRequests.empty()) {
    co_return;
  }
  
  // For single insert, just use regular insert
  if (insertRequests.size() == 1) {
    co_await insert(insertRequests[0]);
    co_return;
  }
  
  // Prepare all buffers first to minimize time holding locks
  std::vector<AlignedBuffer> buffers;
  buffers.reserve(insertRequests.size());
  size_t totalSize = 0;
  
  for (auto& insertRequest : insertRequests) {
    AlignedBuffer buffer;
    
    // Reserve space for entry length (4 bytes)
    buffer.write((uint32_t)0);  // Will be updated with actual length
    
    buffer.write((uint8_t)WALType::Write);
    
    std::string seriesId = insertRequest.seriesKey();
    uint16_t seriesIdLength = seriesId.length();
    buffer.write(seriesIdLength);
    buffer.write(seriesId);
    
    // Value type
    buffer.write((uint8_t) TSM::getValueType<T>());
    
    // Num of timestamps
    buffer.write((uint32_t)insertRequest.timestamps.size());
    
    AlignedBuffer encodedTimestamps = IntegerEncoder::encode(insertRequest.timestamps);
    buffer.write((uint32_t)encodedTimestamps.size());
    buffer.write(encodedTimestamps);
    
    if constexpr (std::is_same<T, double>::value){
      CompressedBuffer encodedFloats = FloatEncoder::encode(insertRequest.values);
      buffer.write((uint32_t)encodedFloats.size());
      buffer.write(encodedFloats);
    } else if constexpr (std::is_same<T, bool>::value){
      AlignedBuffer encodedBools = BoolEncoder::encode(insertRequest.values);
      buffer.write((uint32_t)encodedBools.size());
      buffer.write(encodedBools);
    } else if constexpr (std::is_same<T, std::string>::value){
      AlignedBuffer encodedStrings = StringEncoder::encode(insertRequest.values);
      buffer.write((uint32_t)encodedStrings.size());
      buffer.write(encodedStrings);
    }
    
    // Update the entry length at the beginning
    uint32_t entryLength = buffer.size() - sizeof(uint32_t);
    memcpy(buffer.data.data(), &entryLength, sizeof(uint32_t));
    
    totalSize += buffer.size();
    buffers.push_back(std::move(buffer));
  }
  
  LOG_INSERT_PATH(tsdb::wal_log, debug, "WAL::insertBatch - {} entries, total size={}", buffers.size(), totalSize);
  
  // Check if we need multiple flushes
  size_t bytesWritten = 0;
  for (auto& buffer : buffers) {
    size_t dataSize = buffer.size();
    
    // Check if data fits in current block
    if (bufferPos + dataSize > WAL_BLOCK_SIZE) {
      // Flush current block before writing new data
      co_await flushBlock();
    }
    
    // Copy data to write buffer
    memcpy(writeBuffer.get_write() + bufferPos, buffer.data.data(), dataSize);
    bufferPos += dataSize;
    bytesWritten += dataSize;
    
    // Update current size
    currentSize.fetch_add(dataSize);
  }
  
  // For batch inserts, flush if buffer is more than 50% full
  // This balances between batching efficiency and memory usage
  if (bufferPos > WAL_BLOCK_SIZE * 0.5) {
    co_await flushBlock();
  }
  
  LOG_INSERT_PATH(tsdb::wal_log, debug, "WAL::insertBatch complete - {} bytes written to buffer", bytesWritten);
}

seastar::future<> WAL::deleteRange(const std::string& seriesKey,
                                   uint64_t startTime,
                                   uint64_t endTime) {
  AlignedBuffer buffer;
  
  // Reserve space for entry length (4 bytes)
  buffer.write((uint32_t)0);  // Will be updated with actual length
  
  size_t lengthPos = 0;  // Position where we wrote the length
  
  // Write WAL type
  buffer.write((uint8_t)WALType::DeleteRange);
  
  // Write series key
  uint16_t seriesKeyLength = seriesKey.length();
  buffer.write(seriesKeyLength);
  // AlignedBuffer expects non-const string reference
  std::string seriesKeyCopy = seriesKey;
  buffer.write(seriesKeyCopy);
  
  // Write time range
  buffer.write(startTime);
  buffer.write(endTime);
  
  // Update the entry length at the beginning
  uint32_t entryLength = buffer.size() - 4;  // Exclude the length field itself
  memcpy(buffer.data.data() + lengthPos, &entryLength, 4);
  
  size_t dataSize = buffer.size();
  
  // Check if data fits in current block
  if (bufferPos + dataSize > WAL_BLOCK_SIZE) {
    co_await flushBlock();
  }
  
  // Copy data to write buffer
  memcpy(writeBuffer.get_write() + bufferPos, buffer.data.data(), dataSize);
  bufferPos += dataSize;
  
  // Update current size
  currentSize.fetch_add(dataSize);
  
  LOG_INSERT_PATH(tsdb::wal_log, debug, "WAL deleteRange written: series={}, startTime={}, endTime={}, size={} bytes",
                      seriesKey, startTime, endTime, dataSize);
  
  // Optional: immediate flush for durability
  if (requiresImmediateFlush) {
    co_await flushBlock();
  }
}

void WAL::remove(unsigned int sequenceNumber){
  std::string filename = WAL::sequenceNumberToFilename(sequenceNumber);

  try {
    if (std::filesystem::remove(filename))
       tsdb::wal_log.debug("WAL file {} deleted", filename);
    else
       tsdb::wal_log.debug("WAL file {} not found", filename);
  }
  catch(const std::filesystem::filesystem_error& err) {
     tsdb::wal_log.error("Filesystem error removing WAL: {}", err.what());
  }
}

// TODO: WAL directory should be set in config
WALReader::WALReader(std::string _filename) : filename(_filename) {};

seastar::future<> WALReader::readAll(MemoryStore *store){
  std::string_view filenameView{ filename }; // TODO: string_view as function parameter instead of string
  walFile = co_await seastar::open_file_dma(filenameView, seastar::open_flags::ro);

  if(!walFile){
    tsdb::wal_log.error("Failed to open WAL file: {}", filename);
    throw std::runtime_error("Failed opening WAL file");
  }

  length = co_await walFile.size();
  
  // Align read size for DMA
  size_t alignedLength = ((length + 511) / 512) * 512;
  auto walBuf = co_await walFile.dma_read_exactly<uint8_t>(0, alignedLength);
  Slice walSlice(walBuf.get(), length);  // Use actual file size, not aligned size

  size_t entriesRead = 0;
  size_t partialEntries = 0;
  
  while(walSlice.bytesLeft() > 0){
    // Check if we have enough bytes for entry length
    if (walSlice.bytesLeft() < sizeof(uint32_t)) {
      tsdb::wal_log.warn("WAL recovery: Incomplete entry header at position {}, discarding {} bytes", 
                         length - walSlice.bytesLeft(), walSlice.bytesLeft());
      partialEntries++;
      break;
    }
    
    // Read entry length
    uint32_t entryLength = walSlice.read<uint32_t>();
    
    // Sanity check: WAL entries should never be larger than 100MB
    static constexpr uint32_t MAX_ENTRY_SIZE = 100 * 1024 * 1024; // 100MB
    if (entryLength > MAX_ENTRY_SIZE) {
      tsdb::wal_log.warn("WAL recovery: Corrupted entry at position {}, invalid length {} (max {}), stopping recovery", 
                         length - walSlice.bytesLeft() - sizeof(uint32_t), entryLength, MAX_ENTRY_SIZE);
      partialEntries++;
      break;  // Stop reading, WAL is corrupted from this point
    }
    
    // Check if we have the complete entry
    if (walSlice.bytesLeft() < entryLength) {
      tsdb::wal_log.warn("WAL recovery: Partial entry at position {}, expected {} bytes but only {} available, discarding", 
                         length - walSlice.bytesLeft() - sizeof(uint32_t), entryLength, walSlice.bytesLeft());
      partialEntries++;
      break;  // Stop reading, this is the last partial entry
    }
    
    // Create a sub-slice for this entry
    auto entrySlice = walSlice.getSlice(entryLength);
    
    uint8_t type = entrySlice.read<uint8_t>();

    switch(static_cast<WALType>(type)){
      case WALType::Write: {
        uint16_t seriesIdLength = entrySlice.read<uint16_t>();
        std::string seriesId = entrySlice.readString(seriesIdLength);
        uint8_t valueType = entrySlice.read<uint8_t>();

        switch(static_cast<WALValueType>(valueType)){
          case WALValueType::Float: {
            TSDBInsert<double> insertReq = readSeries<double>(entrySlice, seriesId);
            store->insertMemory(insertReq);
          }
          break;
          case WALValueType::Boolean: {
            TSDBInsert<bool> insertReq = readSeries<bool>(entrySlice, seriesId);
            store->insertMemory(insertReq);
          }
          break;
          case WALValueType::String: {
            TSDBInsert<std::string> insertReq = readSeries<std::string>(entrySlice, seriesId);
            store->insertMemory(insertReq);
          }
          break;
        }
        entriesRead++;
      }
      break;
      case WALType::Close: {
        co_await store->close();
        entriesRead++;
      }
      break;
      case WALType::DeleteRange: {
        // Read series key
        uint16_t seriesKeyLength = entrySlice.read<uint16_t>();
        std::string seriesKey = entrySlice.readString(seriesKeyLength);
        
        // Read time range
        uint64_t startTime = entrySlice.read<uint64_t>();
        uint64_t endTime = entrySlice.read<uint64_t>();
        
        // Apply deletion to memory store
        // Note: For now, we just log it as memory stores don't support deletion yet
        // In a full implementation, we would mark these ranges as deleted
        tsdb::wal_log.debug("WAL recovery: DeleteRange for series={}, startTime={}, endTime={}",
                           seriesKey, startTime, endTime);
        
        // Apply deletion to memory store
        store->deleteRange(seriesKey, startTime, endTime);
        
        entriesRead++;
      }
      break;
      default:
        tsdb::wal_log.warn("WAL recovery: Unknown entry type {} at position {}, stopping recovery", 
                           static_cast<int>(type), length - walSlice.bytesLeft());
        // Stop recovery when we hit an unknown entry type as the WAL is likely corrupted
        partialEntries++;
        goto recovery_complete;  // Break out of the while loop
    }
  }
  
recovery_complete:
  tsdb::wal_log.info("WAL recovery complete: {} entries read, {} partial entries discarded", 
                      entriesRead, partialEntries);
}

template <class T>
TSDBInsert<T> WALReader::readSeries(Slice &walSlice, std::string &seriesId){
  TSDBInsert<T> insertReq = TSDBInsert<T>::fromSeriesKey(seriesId);

  uint32_t timestampsCount = walSlice.read<uint32_t>();
  uint32_t encodedTimestampsSize = walSlice.read<uint32_t>();

  auto timestampsSlice = walSlice.getSlice(encodedTimestampsSize);
  auto [nSkipped, nTimestamps] = IntegerEncoder::decode(timestampsSlice, timestampsCount, insertReq.timestamps);
  uint32_t valueByteSize = walSlice.read<uint32_t>();

  if constexpr (std::is_same<T, bool>::value){
    auto valuesSlice = walSlice.getSlice(valueByteSize);
    BoolEncoder::decode(valuesSlice, nSkipped, nTimestamps, insertReq.values);
  } else if constexpr (std::is_same<T, double>::value) {
    auto valuesSlice = walSlice.getCompressedSlice(valueByteSize);

    FloatEncoder::decode(valuesSlice, nSkipped, nTimestamps, insertReq.values);
  } else if constexpr (std::is_same<T, std::string>::value) {
    auto valuesSlice = walSlice.getSlice(valueByteSize);
    std::vector<std::string> allStrings;
    StringEncoder::decode(valuesSlice, timestampsCount, allStrings);
    
    // Skip and take the appropriate strings
    insertReq.values.reserve(nTimestamps);
    for (size_t i = nSkipped; i < nSkipped + nTimestamps && i < allStrings.size(); i++) {
      insertReq.values.push_back(std::move(allStrings[i]));
    }
  } else {
    throw std::runtime_error("Unsupported data type");
  }

  return std::move(insertReq);
}

template seastar::future<> WAL::insert<double>(TSDBInsert<double> &insertRequest);
template seastar::future<> WAL::insert<bool>(TSDBInsert<bool> &insertRequest);
template seastar::future<> WAL::insert<std::string>(TSDBInsert<std::string> &insertRequest);
template size_t WAL::estimateInsertSize<double>(TSDBInsert<double> &insertRequest);
template size_t WAL::estimateInsertSize<bool>(TSDBInsert<bool> &insertRequest);
template size_t WAL::estimateInsertSize<std::string>(TSDBInsert<std::string> &insertRequest);
template seastar::future<> WAL::insertBatch<double>(std::vector<TSDBInsert<double>>& insertRequests);
template seastar::future<> WAL::insertBatch<bool>(std::vector<TSDBInsert<bool>>& insertRequests);
template seastar::future<> WAL::insertBatch<std::string>(std::vector<TSDBInsert<std::string>>& insertRequests);