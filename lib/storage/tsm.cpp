#include "tsm.hpp"
#include "float_encoder.hpp"
#include "integer_encoder.hpp"
#include "bool_encoder.hpp"
#include "string_encoder.hpp"
#include "slice_buffer.hpp"
#include "logger.hpp"

#include <filesystem>
#include <algorithm>
#include <string_view>

#include <chrono>

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/loop.hh>

typedef std::chrono::high_resolution_clock Clock;



TSM::TSM(std::string _absoluteFilePath){
  size_t filenameEndIndex = _absoluteFilePath.find_last_of(".");
  size_t filenameStartIndex = _absoluteFilePath.find_last_of("/") + 1;

  std::string filename = _absoluteFilePath.substr(filenameStartIndex, filenameEndIndex - filenameStartIndex);

  size_t underscoreIndex = filename.find_last_of("_");
  if(underscoreIndex > filename.length())
    throw std::runtime_error("TSM invalid filename:" + filename);

  try {
    tierNum = std::stoi(filename.substr(0, underscoreIndex));
    seqNum = std::stoi(filename.substr(underscoreIndex+1));

    tsdb::tsm_log.debug("tierNum={} seqNum={}", tierNum, seqNum);
  } catch(const std::exception&) {
    throw std::runtime_error("TSM invalid filename:" + filename);
  }

  filePath = _absoluteFilePath;
}

seastar::future<> TSM::open(){
  std::string_view filePathView{ filePath };
  tsmFile = co_await seastar::open_file_dma(filePathView, seastar::open_flags::ro);

  if(!tsmFile){
    tsdb::tsm_log.error("TSM unable to open: {}", filePath);
    throw std::runtime_error("TSM unable to open:" + filePath);
  }

  length = co_await tsmFile.size();

  co_await readIndex();
  
  // Load tombstones if they exist
  co_await loadTombstones();
};

seastar::future<> TSM::close() {
  if (tsmFile) {
    co_await tsmFile.close();
  }
  co_return;
}

uint64_t TSM::rankAsInteger(){
  return (tierNum << 60) + seqNum;
}

seastar::future<> TSM::readIndex(){  
  auto indexOffsetBuf = co_await tsmFile.dma_read_exactly<uint8_t>(length - sizeof(size_t), sizeof(size_t));
  size_t indexOffset = *(size_t*)(indexOffsetBuf.get()); 
  
  auto indexBuf = co_await tsmFile.dma_read_exactly<uint8_t>(indexOffset, length - indexOffset - sizeof(size_t));
  Slice indexSlice(indexBuf.get(), indexBuf.size());

  while(indexSlice.offset < indexSlice.length_){
    TSMIndexEntry indexEntry;

    uint16_t seriesIdLength = indexSlice.read<uint16_t>();
    indexEntry.seriesId = indexSlice.readString(seriesIdLength);
    indexEntry.seriesType = (TSMValueType) indexSlice.read<uint8_t>();

    uint16_t indexEntryCount = indexSlice.read<uint16_t>();

    for(int i=0; i < indexEntryCount; i++){
      TSMIndexBlock indexBlock;
      indexBlock.minTime = indexSlice.read<uint64_t>();
      indexBlock.maxTime = indexSlice.read<uint64_t>();
      indexBlock.offset = indexSlice.read<uint64_t>();
      indexBlock.size = indexSlice.read<uint32_t>();

      indexEntry.indexBlocks.push_back(indexBlock);
    }

    index.insert({indexEntry.seriesId, indexEntry});
  }
}

template <class T>
seastar::future<> TSM::readSeries(std::string seriesKey, uint64_t startTime, uint64_t endTime, TSMResult<T> &results){
  auto it = index.find(seriesKey);

  if(it == index.end())
    co_return;

  std::vector<TSMIndexBlock> blocksToScan;
  std::copy_if(
    it->second.indexBlocks.begin(),
    it->second.indexBlocks.end(),
    std::back_inserter(blocksToScan),
    [endTime, startTime](TSMIndexBlock indexBlock){
      return indexBlock.minTime <= endTime && startTime <= indexBlock.maxTime;
    }
  );

  co_await seastar::parallel_for_each(blocksToScan, [&] (TSMIndexBlock indexBlock) {
    return readBlock(indexBlock, startTime, endTime, results);
  });
}

template <class T>
seastar::future<> TSM::readBlock(const TSMIndexBlock &indexBlock, uint64_t startTime, uint64_t endTime, TSMResult<T> &results){
  auto blockBuf = co_await tsmFile.dma_read_exactly<uint8_t>(indexBlock.offset, indexBlock.size);
  Slice blockSlice(blockBuf.get(), blockBuf.size());

  auto headerSlice = blockSlice.getSlice(9);
  uint8_t blockType = headerSlice.read<uint8_t>();
  uint32_t timestampSize = headerSlice.read<uint32_t>();
  uint32_t timestampBytes = headerSlice.read<uint32_t>();

  auto blockResults = std::make_unique<TSMBlock<T>>(timestampSize);
  auto timestampsSlice = blockSlice.getSlice(timestampBytes);
  auto [nSkipped, nTimestamps] = IntegerEncoder::decode(timestampsSlice, timestampSize, *blockResults->timestamps, startTime, endTime);
  size_t valueByteSize = indexBlock.size - timestampBytes - (sizeof(uint8_t) + 2*sizeof(uint32_t));

  if constexpr (std::is_same<T, double>::value)
  {
    auto valuesSlice = blockSlice.getCompressedSlice(valueByteSize);
    FloatEncoder::decode(valuesSlice, nSkipped, nTimestamps, *blockResults->values);

  } else if constexpr (std::is_same<T, bool>::value) {
    auto valuesSlice = blockSlice.getSlice(valueByteSize);
    BoolEncoder::decode(valuesSlice, nSkipped, nTimestamps, *blockResults->values);
    
  } else if constexpr (std::is_same<T, std::string>::value) {
    auto valuesSlice = blockSlice.getSlice(valueByteSize);
    std::vector<std::string> allStrings;
    StringEncoder::decode(valuesSlice, timestampSize, allStrings);
    
    // Skip and take the appropriate strings based on nSkipped and nTimestamps
    blockResults->values->reserve(nTimestamps);
    for (size_t i = nSkipped; i < nSkipped + nTimestamps && i < allStrings.size(); i++) {
      blockResults->values->push_back(std::move(allStrings[i]));
    }
  }

  results.appendBlock(blockResults);
}

std::optional<TSMValueType> TSM::getSeriesType(std::string &seriesKey){
  auto it = index.find(seriesKey);
  
  if(it == index.end())
    return {};
  
  return it->second.seriesType;
}

template seastar::future<> TSM::readSeries<double>(std::string seriesKey, uint64_t startTime, uint64_t endTime, TSMResult<double> &results);
template seastar::future<> TSM::readSeries<bool>(std::string seriesKey, uint64_t startTime, uint64_t endTime, TSMResult<bool> &results);
template seastar::future<> TSM::readSeries<std::string>(std::string seriesKey, uint64_t startTime, uint64_t endTime, TSMResult<std::string> &results);

seastar::future<> TSM::scheduleDelete() {
    // Close the file if it's open
    if (tsmFile) {
        co_await tsmFile.close();
    }
    
    // Delete the physical file
    try {
        std::filesystem::remove(filePath);
        tsdb::tsm_log.info("TSM file deleted: {}", filePath);
    } catch (const std::exception& e) {
        tsdb::tsm_log.error("Failed to delete TSM file {}: {}", filePath, e.what());
    }
    
    co_return;
}

