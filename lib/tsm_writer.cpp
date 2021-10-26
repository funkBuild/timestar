#include "tsm.hpp"
#include "tsm_writer.hpp"
#include "integer_encoder.hpp"
#include "float_encoder.hpp"
#include "bool_encoder.hpp"

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
void TSMWriter::writeSeries(TSMValueType seriesType, const std::string &seriesId, const std::vector<uint64_t> &timestamps, const std::vector<T> &values){
  // serializes a single series into one or more blocks. After each block, append an index entry
  // TODO: What's the optimum block size??
  TSMIndexEntry indexEntry;
  indexEntry.seriesId = seriesId;
  indexEntry.seriesType = seriesType;

  unsigned int offset = 0;

  while(offset < timestamps.size()){
    const size_t end = std::min(timestamps.size(), (size_t)(offset + MaxPointsPerBlock));

    std::vector<uint64_t> blockTimestamps(timestamps.begin() + offset, timestamps.begin() + end);
    std::vector<T> blockValues(values.begin() + offset, values.begin() + end);

    // TODO: Implement bool encoded
    writeBlock(seriesType, seriesId, blockTimestamps, blockValues, indexEntry);

    offset += MaxPointsPerBlock;
  }

  indexEntries.push_back(std::move(indexEntry));
}

template <class T>
void TSMWriter::writeBlock(TSMValueType seriesType, const std::string &seriesId, const std::vector<uint64_t> &timestamps, const std::vector<T> &values, TSMIndexEntry &indexEntry){
  size_t blockStartOffset = buffer.size();
  AlignedBuffer encodedTimestamps = IntegerEncoder::encode(timestamps);

  buffer.write((uint8_t)seriesType);  // uint8_t fieldType
  buffer.write((uint32_t)timestamps.size());  // uint32_t timestamp partition length in bytes
  buffer.write((uint32_t)encodedTimestamps.size());  // uint32_t timestamp partition length in bytes

  buffer.write(encodedTimestamps);  // uint8_t x N bytes, compressed timestamps

  if constexpr (std::is_same<T, double>::value){
    CompressedBuffer encodedFloats = FloatEncoder::encode(values);
    buffer.write(encodedFloats);  // uint8_t x N bytes, compressed values

  } else if constexpr (std::is_same<T, bool>::value){
    AlignedBuffer encodedBools = BoolEncoder::encode(values);
    buffer.write(encodedBools);  // uint8_t x N bytes, compressed values

  } else {
    throw std::runtime_error("Unsupported data type");
  }
  
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

  std::cout << "blockStart=" << blockStartOffset << " blockSize=" << blockSize << std::endl;
  std::cout << "minTime=" << *minTime << " maxTime=" << *maxTime << std::endl;
}

void TSMWriter::writeIndex(){
  // Write each index entry that points to a block
  std::sort(indexEntries.begin(), indexEntries.end());

  size_t indexStartOffset = buffer.size();

  for(auto const& indexEntry: indexEntries){
    // Key length
    uint16_t seriesIdLength = indexEntry.seriesId.length();
    buffer.write(seriesIdLength);

    // Key value as string
    std::string seriesId = indexEntry.seriesId;
    buffer.write(seriesId);

    // Block type
    // TODO: Write the correct block type to the index
    buffer.write((uint8_t)TSMValueType::Float);  // uint8_t fieldType

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

  buffer.write(indexStartOffset);
}

void TSMWriter::close(){
  std::ofstream file(filename, std::ios::binary);
  file << buffer;
}

void TSMWriter::run(std::shared_ptr<MemoryStore> store, std::string filename){
  TSMWriter writer(filename);

  for (auto& [seriesId, memStore] : store.get()->series){
    // TODO: Support multiple types
    std::cout << "TSMWriter start" << std::endl;

    TSMValueType seriesType = (TSMValueType) memStore.index();
    switch(seriesType){
      case TSMValueType::Float: {
        auto series = std::get<InMemorySeries<double>>(memStore);
        series.sort();
        writer.writeSeries(seriesType, seriesId, series.timestamps, series.values);
      }
      break;
      case TSMValueType::Boolean: {
        std::cout << "Got bool series" << std::endl;
        auto series = std::get<InMemorySeries<bool>>(memStore);
        series.sort();
        writer.writeSeries(seriesType, seriesId, series.timestamps, series.values);
      }
      break;
    }


  }

  writer.writeIndex();
  writer.close();
}
