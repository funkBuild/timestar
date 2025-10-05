#include "tsm.hpp"
#include "tsm_writer.hpp"
#include "series_id.hpp"
#include "integer_encoder.hpp"
#include "float_encoder.hpp"
#include "bool_encoder.hpp"
#include "string_encoder.hpp"

#include <iostream>
#include <algorithm>
#include <variant>

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

  unsigned int offset = 0;
  size_t blockCount = 0;

  while(offset < timestamps.size()){
    const size_t end = std::min(timestamps.size(), (size_t)(offset + MaxPointsPerBlock));
    size_t blockSize = end - offset;
    
    if (blockCount == 0) {
      std::cerr << "[TSM_WRITER_BLOCK] Creating blocks for series '" << seriesId.toHex() 
                << "' (" << timestamps.size() << " total points, up to " << MaxPointsPerBlock << " per block)" << std::endl;
    }

    //TODO: Avoid the copy here
    std::cerr << "[TSM_WRITER_BLOCK] Allocating vectors for block " << blockCount << " (" << blockSize << " points)" << std::endl;
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
    std::cerr << "[TSM_WRITER_DIRECT] Warning: series has " << timestamps.size()
              << " points (>" << MaxPointsPerBlock << "), writing as multiple blocks with fallback" << std::endl;

    // Fallback to copy-based approach for multi-block series
    // (Move-from vectors become lvalues, safe to pass as const&)
    writeSeries(seriesType, seriesId, timestamps, values);
    return;
  }

  // Single block - use move semantics path
  TSMIndexEntry indexEntry;
  indexEntry.seriesId = seriesId;
  indexEntry.seriesType = seriesType;

  std::cerr << "[TSM_WRITER_DIRECT] Zero-copy write for series '" << seriesId.toHex()
            << "' (" << timestamps.size() << " points)" << std::endl;

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
  std::cerr << "[TSM_WRITER_INDEX] Index starts at offset: " << indexStartOffset
            << " (0x" << std::hex << indexStartOffset << std::dec << ")" << std::endl;
  std::cerr << "[TSM_WRITER_INDEX] Writing " << indexEntries.size() << " index entries" << std::endl;

  // Iterate directly - already sorted by SeriesId128
  for(auto const& [seriesId, indexEntry]: indexEntries){
    // Write SeriesId128 as 16 bytes (no length prefix needed since it's fixed size)
    std::string seriesIdBytes = indexEntry.seriesId.toBytes();
    buffer.write(seriesIdBytes);

    // Block type
    buffer.write((uint8_t)indexEntry.seriesType);  // uint8_t fieldType

    // num of index entries
    buffer.write((uint16_t)indexEntry.indexBlocks.size());

    // for each block
    for(auto const& block: indexEntry.indexBlocks){
      buffer.write(block.minTime); // minTime
      buffer.write(block.maxTime); // maxTime
      buffer.write(block.offset); // byte offset from start of file
      buffer.write(block.size); // block size
    }
  }

  std::cerr << "[TSM_WRITER_INDEX] Buffer size before writing offset: " << buffer.size() << std::endl;
  buffer.write(indexStartOffset);
  std::cerr << "[TSM_WRITER_INDEX] Buffer size after writing offset: " << buffer.size() << std::endl;
  std::cerr << "[TSM_WRITER_INDEX] Wrote index offset: " << indexStartOffset
            << " (0x" << std::hex << indexStartOffset << std::dec << ")" << std::endl;
}

// Phase 4A: Parallel index building (placeholder for future enhancement)
void TSMWriter::writeIndexParallel(){
  // For now, just call the regular writeIndex
  // Future: parallelize serialization of index entries
  writeIndex();
}

void TSMWriter::close(){
  std::cerr << "[TSM_WRITER_CLOSE] Writing file: " << filename << std::endl;
  std::cerr << "[TSM_WRITER_CLOSE] Buffer size: " << buffer.size()
            << " (0x" << std::hex << buffer.size() << std::dec << ")" << std::endl;
  std::cerr << "[TSM_WRITER_CLOSE] Buffer capacity: " << buffer.capacity() << std::endl;

  std::ofstream file(filename, std::ios::binary);
  file << buffer;
  file.flush();

  std::cerr << "[TSM_WRITER_CLOSE] File written successfully" << std::endl;
}

void TSMWriter::run(seastar::shared_ptr<MemoryStore> store, std::string filename){
  std::cerr << "[TSM_WRITER] Starting TSM write to file: " << filename << std::endl;
  std::cerr << "[TSM_WRITER] Memory store has " << store.get()->series.size() << " series" << std::endl;
  
  TSMWriter writer(filename);
  size_t seriesProcessed = 0;
  size_t totalPoints = 0;

  for (auto& [seriesKey, memStore] : store.get()->series){
    TSMValueType seriesType = (TSMValueType) memStore.index();
    
    size_t seriesPoints = std::visit([](const auto& s) { return s.timestamps.size(); }, memStore);
    std::cerr << "[TSM_WRITER] Processing series '" << seriesKey.toHex() << "' with " << seriesPoints << " points, type=" << static_cast<int>(seriesType) << std::endl;
    
    // seriesKey is now SeriesId128
    SeriesId128 seriesId = seriesKey;
    
    try {
      switch(seriesType){
        case TSMValueType::Float: {
          auto series = std::get<InMemorySeries<double>>(memStore);
          std::cerr << "[TSM_WRITER] Sorting float series '" << seriesKey.toHex() << "'" << std::endl;
          series.sort();
          std::cerr << "[TSM_WRITER] Writing float series '" << seriesKey.toHex() << "' with " << series.timestamps.size() << " points" << std::endl;
          writer.writeSeries(seriesType, seriesId, series.timestamps, series.values);
        }
        break;
        case TSMValueType::Boolean: {
          auto series = std::get<InMemorySeries<bool>>(memStore);
          std::cerr << "[TSM_WRITER] Sorting bool series '" << seriesKey.toHex() << "'" << std::endl;
          series.sort();
          std::cerr << "[TSM_WRITER] Writing bool series '" << seriesKey.toHex() << "' with " << series.timestamps.size() << " points" << std::endl;
          writer.writeSeries(seriesType, seriesId, series.timestamps, series.values);
        }
        break;
        case TSMValueType::String: {
          auto series = std::get<InMemorySeries<std::string>>(memStore);
          std::cerr << "[TSM_WRITER] Sorting string series '" << seriesKey.toHex() << "' with " << series.timestamps.size() << " points" << std::endl;
          series.sort();
          std::cerr << "[TSM_WRITER] Writing string series '" << seriesKey.toHex() << "' with " << series.timestamps.size() << " points" << std::endl;
          writer.writeSeries(seriesType, seriesId, series.timestamps, series.values);
        }
        break;
      }
    } catch (const std::bad_alloc& e) {
      std::cerr << "[TSM_WRITER] BAD_ALLOC when processing series '" << seriesKey.toHex() << "' with " << seriesPoints << " points" << std::endl;
      throw;
    } catch (const std::exception& e) {
      std::cerr << "[TSM_WRITER] ERROR processing series '" << seriesKey.toHex() << "': " << e.what() << std::endl;
      throw;
    }
    
    seriesProcessed++;
    totalPoints += seriesPoints;
  }

  std::cerr << "[TSM_WRITER] Writing index..." << std::endl;
  writer.writeIndex();
  std::cerr << "[TSM_WRITER] Closing file..." << std::endl;
  writer.close();
  std::cerr << "[TSM_WRITER] TSM write complete. Processed " << seriesProcessed << " series with " << totalPoints << " total points" << std::endl;
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
