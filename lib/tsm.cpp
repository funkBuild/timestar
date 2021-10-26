#include "tsm.hpp"
#include "float_encoder.hpp"
#include "integer_encoder.hpp"
#include "bool_encoder.hpp"
#include "slice_buffer.hpp"

#include <iostream>
#include <filesystem>
#include <algorithm>

#include <chrono>


typedef std::chrono::high_resolution_clock Clock;



TSM::TSM(std::string absolutePath){
  size_t filenameEndIndex = absolutePath.find_last_of(".");
  size_t filenameStartIndex = absolutePath.find_last_of("/") + 1;

  filename = absolutePath.substr(filenameStartIndex, filenameEndIndex - filenameStartIndex);

  size_t underscoreIndex = filename.find_last_of("_");
  if(underscoreIndex > filename.length())
    throw std::runtime_error("TSM invalid filename:" + filename);

  try {
    tierNum = std::stoi(filename.substr(0, underscoreIndex));
    seqNum = std::stoi(filename.substr(underscoreIndex+1));
  } catch(const std::exception&) {
    throw std::runtime_error("TSM invalid filename:" + filename);
  }

  tsmFile = std::make_unique<std::ifstream>(absolutePath, std::ios::binary);

  if(!tsmFile->is_open()){
    std::cout << "TSM unable to open:" << absolutePath << std::endl;
    throw std::runtime_error("TSM unable to open:" + absolutePath);
  }

  tsmFile->seekg(0, tsmFile->end);
  length = tsmFile->tellg();
  tsmFile->seekg(0, tsmFile->beg);

  readIndex();
}

void TSM::readIndex(){
  // TODO: Convert to using slices

  tsmFile->seekg(-1 * sizeof(size_t), tsmFile->end);

  size_t indexOffset;
  tsmFile->read((char*)&indexOffset, sizeof(size_t));
  tsmFile->seekg(indexOffset, tsmFile->beg); // Seek to index start

  while(tsmFile->tellg() < length - sizeof(size_t)){ // End of file - sizeof(size_t) for the index offset
    TSMIndexEntry indexEntry;

    uint16_t seriesIdLength;
    tsmFile->read((char*)&seriesIdLength, 2);

    std::string seriesId(seriesIdLength, ' ');
    tsmFile->read(&seriesId[0], seriesIdLength);
    indexEntry.seriesId = seriesId;


    tsmFile->read((char*)&indexEntry.seriesType, sizeof(uint8_t));

    uint16_t indexEntryCount;
    tsmFile->read((char*)&indexEntryCount, sizeof(uint16_t));

    for(int i=0; i < indexEntryCount; i++){
      TSMIndexBlock indexBlock;
      tsmFile->read((char*)&indexBlock.minTime, sizeof(uint64_t));
      tsmFile->read((char*)&indexBlock.maxTime, sizeof(uint64_t));
      tsmFile->read((char*)&indexBlock.offset, sizeof(uint64_t));
      tsmFile->read((char*)&indexBlock.size, sizeof(uint32_t));

      indexEntry.indexBlocks.push_back(indexBlock);
    }

    index.insert({indexEntry.seriesId, indexEntry});
  }
}

template <class T>
void TSM::readSeries(std::string seriesKey, uint64_t startTime, uint64_t endTime, QueryResult<T> &results){
  auto it = index.find(seriesKey);

  if(it == index.end())
    return;
  
  std::vector<TSMIndexBlock> blocksToScan;
  std::copy_if(
    it->second.indexBlocks.begin(),
    it->second.indexBlocks.end(),
    std::back_inserter(blocksToScan),
    [endTime, startTime](TSMIndexBlock indexBlock){
      return indexBlock.minTime <= endTime && startTime <= indexBlock.maxTime;
    }
  );

  for(auto const& indexBlock: blocksToScan){
    readBlock(indexBlock, startTime, endTime, results);
  }
}

template <class T>
void TSM::readBlock(const TSMIndexBlock &indexBlock, uint64_t startTime, uint64_t endTime, QueryResult<T> &results){
  SliceBuffer buffer(indexBlock.size);

  tsmFile->seekg(indexBlock.offset, tsmFile->beg);
  tsmFile->read((char*)&buffer.data[0], indexBlock.size);

  auto headerSlice = buffer.getSlice(0, 5);

  uint8_t blockType = headerSlice.read<uint8_t>();
  uint32_t timestampSize = headerSlice.read<uint32_t>();
  uint32_t timestampBytes = headerSlice.read<uint32_t>();

  results.extendCapacity(timestampSize);

  auto timestampsSlice = buffer.getSlice(9, timestampBytes);

  auto [nSkipped, nTimestamps] = IntegerEncoder::decode(timestampsSlice, timestampSize, *results.timestamps, startTime, endTime);
  size_t valueByteSize = indexBlock.size - timestampBytes - 5;

  tsmFile->seekg(indexBlock.offset + 5 + timestampBytes, tsmFile->beg);

  if constexpr (std::is_same<T, double>::value)
  {
    auto valuesSlice = buffer.getCompressedSlice(5 + timestampBytes, valueByteSize);

    // TODO: Validate usage of compressedSlice
    FloatEncoder::decode(valuesSlice, nSkipped, nTimestamps, *results.values);
  } else if constexpr (std::is_same<T, bool>::value) {
    auto valuesSlice = buffer.getSlice(5 + timestampBytes, valueByteSize);

    BoolEncoder::decode(valuesSlice, nSkipped, nTimestamps, *results.values);
  }
}

std::optional<TSMValueType> TSM::getSeriesType(std::string seriesKey){
  auto it = index.find(seriesKey);
  
  if(it == index.end())
    return {};
  
  return it->second.seriesType;
}

template void TSM::readSeries<double>(std::string seriesKey, uint64_t startTime, uint64_t endTime, QueryResult<double> &results);
template void TSM::readSeries<bool>(std::string seriesKey, uint64_t startTime, uint64_t endTime, QueryResult<bool> &results);

