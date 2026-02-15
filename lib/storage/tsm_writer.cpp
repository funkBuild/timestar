#include "tsm.hpp"
#include "tsm_writer.hpp"
#include "series_id.hpp"
#include "integer_encoder.hpp"
#include "float_encoder.hpp"
#include "bool_encoder.hpp"
#include "string_encoder.hpp"

#include "logger.hpp"
#include "logging_config.hpp"

#include <algorithm>
#include <limits>
#include <variant>

#include <fcntl.h>
#include <unistd.h>

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
    const size_t end = std::min(timestamps.size(), (size_t)(offset + MaxPointsPerBlock));
    size_t blockSize = end - offset;
    
    if (blockCount == 0) {
      LOG_INSERT_PATH(tsdb::tsm_log, debug, "Creating blocks for series '{}' ({} total points, up to {} per block)",
                      seriesId.toHex(), timestamps.size(), MaxPointsPerBlock);
    }

    //TODO: Avoid the copy here
    LOG_INSERT_PATH(tsdb::tsm_log, trace, "Allocating vectors for block {} ({} points)", blockCount, blockSize);
    std::vector<uint64_t> blockTimestamps(timestamps.begin() + offset, timestamps.begin() + end);
    std::vector<T> blockValues(values.begin() + offset, values.begin() + end);
    blockCount++;

    // TODO: Implement bool encoded
    writeBlock(seriesType, seriesId, blockTimestamps, blockValues, indexEntry);

    offset += MaxPointsPerBlock;
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

  if constexpr (std::is_same<T, double>::value){
    CompressedBuffer encodedFloats = FloatEncoder::encode(values);
    buffer.write(encodedFloats);  // uint8_t x N bytes, compressed values

  } else if constexpr (std::is_same<T, bool>::value){
    AlignedBuffer encodedBools = BoolEncoder::encode(values);
    buffer.write(encodedBools);  // uint8_t x N bytes, compressed values

  } else if constexpr (std::is_same<T, std::string>::value){
    AlignedBuffer encodedStrings = StringEncoder::encode(values);
    buffer.write(encodedStrings);  // uint8_t x N bytes, compressed values

  } else {
    throw std::runtime_error("Unsupported data type");
  }

  writeIndexBlock(timestamps, indexEntry, blockStartOffset);
}

// Phase 3.2: Move semantics version for zero-copy writes from batch pool
template <class T>
void TSMWriter::writeSeriesDirect(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<T> &&values) {
  // Zero-copy write - caller transfers ownership of vectors
  // Assumes data is already properly sized (single block or caller handles splitting)

  if (timestamps.size() > MaxPointsPerBlock) {
    LOG_INSERT_PATH(tsdb::tsm_log, debug, "Series has {} points (>{}), writing as multiple blocks with fallback",
                    timestamps.size(), MaxPointsPerBlock);

    // Fallback to copy-based approach for multi-block series
    // (Move-from vectors become lvalues, safe to pass as const&)
    writeSeries(seriesType, seriesId, timestamps, values);
    return;
  }

  // Single block - use move semantics path
  TSMIndexEntry indexEntry;
  indexEntry.seriesId = seriesId;
  indexEntry.seriesType = seriesType;

  LOG_INSERT_PATH(tsdb::tsm_log, debug, "Zero-copy write for series '{}' ({} points)",
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
  if constexpr (std::is_same<T, double>::value) {
    CompressedBuffer encodedFloats = FloatEncoder::encode(values);
    buffer.write(encodedFloats);
  } else if constexpr (std::is_same<T, bool>::value) {
    AlignedBuffer encodedBools = BoolEncoder::encode(values);
    buffer.write(encodedBools);
  } else if constexpr (std::is_same<T, std::string>::value) {
    AlignedBuffer encodedStrings = StringEncoder::encode(values);
    buffer.write(encodedStrings);
  } else {
    throw std::runtime_error("Unsupported data type");
  }

  // Write index block (timestamps still valid as lvalue)
  writeIndexBlock(timestamps, indexEntry, blockStartOffset);
}

void TSMWriter::writeIndexBlock(const std::vector<uint64_t> &timestamps, TSMIndexEntry &indexEntry, size_t blockStartOffset){
  const auto [minTime, maxTime] = std::minmax_element(begin(timestamps), end(timestamps));
  size_t blockSize = buffer.size() - blockStartOffset;

  TSMIndexBlock indexBlock;
  indexBlock.minTime = *minTime;
  indexBlock.maxTime = *maxTime;
  indexBlock.offset = blockStartOffset;
  indexBlock.size = blockSize;

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
  TSMIndexBlock indexBlock;
  indexBlock.minTime = minTime;
  indexBlock.maxTime = maxTime;
  indexBlock.offset = blockStartOffset;
  indexBlock.size = compressedData.size();

  indexEntry.indexBlocks.push_back(std::move(indexBlock));
}

void TSMWriter::writeIndex(){
  // std::map maintains sorted order automatically
  size_t indexStartOffset = buffer.size();
  LOG_INSERT_PATH(tsdb::tsm_log, debug, "Index starts at offset: {} ({:#x}), writing {} index entries",
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
  LOG_INSERT_PATH(tsdb::tsm_log, debug, "Wrote index offset: {} ({:#x}), final buffer size: {}",
                  indexStartOffset, indexStartOffset, buffer.size());
}

// Phase 4A: Parallel index building (placeholder for future enhancement)
void TSMWriter::writeIndexParallel(){
  // For now, just call the regular writeIndex
  // Future: parallelize serialization of index entries
  writeIndex();
}

void TSMWriter::close(){
  LOG_INSERT_PATH(tsdb::tsm_log, debug, "Writing file: {}, buffer size: {} ({:#x}), capacity: {}",
                  filename, buffer.size(), buffer.size(), buffer.capacity());

  std::ofstream file(filename, std::ios::binary);
  file << buffer;
  file.flush();
  file.close();

  // Ensure data is durable on disk
  int fd = ::open(filename.c_str(), O_RDONLY);
  if (fd >= 0) {
    ::fsync(fd);
    ::close(fd);
  }

  LOG_INSERT_PATH(tsdb::tsm_log, debug, "File written successfully: {}", filename);
}

void TSMWriter::run(seastar::shared_ptr<MemoryStore> store, std::string filename){
  LOG_INSERT_PATH(tsdb::tsm_log, info, "Starting TSM write to file: {}, memory store has {} series",
                  filename, store.get()->series.size());
  
  TSMWriter writer(filename);
  size_t seriesProcessed = 0;
  size_t totalPoints = 0;

  for (auto it = store.get()->series.begin(); it != store.get()->series.end(); ++it){
    const auto& seriesKey = it->first;
    auto& memStore = it.value();
    TSMValueType seriesType = (TSMValueType) memStore.index();
    
    size_t seriesPoints = std::visit([](const auto& s) { return s.timestamps.size(); }, memStore);
    LOG_INSERT_PATH(tsdb::tsm_log, debug, "Processing series '{}' with {} points, type={}",
                    seriesKey.toHex(), seriesPoints, static_cast<int>(seriesType));
    
    // seriesKey is now SeriesId128
    SeriesId128 seriesId = seriesKey;
    
    try {
      switch(seriesType){
        case TSMValueType::Float: {
          auto& series = std::get<InMemorySeries<double>>(memStore);
          series.sort();
          LOG_INSERT_PATH(tsdb::tsm_log, trace, "Writing float series '{}' with {} points",
                          seriesKey.toHex(), series.timestamps.size());
          writer.writeSeries(seriesType, seriesId, series.timestamps, series.values);
        }
        break;
        case TSMValueType::Boolean: {
          auto& series = std::get<InMemorySeries<bool>>(memStore);
          series.sort();
          LOG_INSERT_PATH(tsdb::tsm_log, trace, "Writing bool series '{}' with {} points",
                          seriesKey.toHex(), series.timestamps.size());
          writer.writeSeries(seriesType, seriesId, series.timestamps, series.values);
        }
        break;
        case TSMValueType::String: {
          auto& series = std::get<InMemorySeries<std::string>>(memStore);
          series.sort();
          LOG_INSERT_PATH(tsdb::tsm_log, trace, "Writing string series '{}' with {} points",
                          seriesKey.toHex(), series.timestamps.size());
          writer.writeSeries(seriesType, seriesId, series.timestamps, series.values);
        }
        break;
      }
    } catch (const std::bad_alloc& e) {
      tsdb::tsm_log.error("BAD_ALLOC when processing series '{}' with {} points", seriesKey.toHex(), seriesPoints);
      throw;
    } catch (const std::exception& e) {
      tsdb::tsm_log.error("ERROR processing series '{}': {}", seriesKey.toHex(), e.what());
      throw;
    }
    
    seriesProcessed++;
    totalPoints += seriesPoints;
  }

  LOG_INSERT_PATH(tsdb::tsm_log, debug, "Writing index...");
  writer.writeIndex();
  LOG_INSERT_PATH(tsdb::tsm_log, debug, "Closing file...");
  writer.close();
  LOG_INSERT_PATH(tsdb::tsm_log, info, "TSM write complete. Processed {} series with {} total points",
                  seriesProcessed, totalPoints);
}

// Template instantiations
template void TSMWriter::writeSeries<double>(TSMValueType seriesType, const SeriesId128 &seriesId, const std::vector<uint64_t> &timestamps, const std::vector<double> &values);
template void TSMWriter::writeSeries<bool>(TSMValueType seriesType, const SeriesId128 &seriesId, const std::vector<uint64_t> &timestamps, const std::vector<bool> &values);
template void TSMWriter::writeSeries<std::string>(TSMValueType seriesType, const SeriesId128 &seriesId, const std::vector<uint64_t> &timestamps, const std::vector<std::string> &values);

// Phase 3.2: Template instantiations for move semantics versions
template void TSMWriter::writeSeriesDirect<double>(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<double> &&values);
template void TSMWriter::writeSeriesDirect<bool>(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<bool> &&values);
template void TSMWriter::writeSeriesDirect<std::string>(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<std::string> &&values);

template void TSMWriter::writeBlockDirect<double>(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<double> &&values, TSMIndexEntry &indexEntry);
template void TSMWriter::writeBlockDirect<bool>(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<bool> &&values, TSMIndexEntry &indexEntry);
template void TSMWriter::writeBlockDirect<std::string>(TSMValueType seriesType, const SeriesId128 &seriesId, std::vector<uint64_t> &&timestamps, std::vector<std::string> &&values, TSMIndexEntry &indexEntry);
