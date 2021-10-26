#ifndef __TSM_H_INCLUDED__
#define __TSM_H_INCLUDED__

#include "query_result.hpp"

#include <string>
#include <memory>
#include <fstream>
#include <unordered_map>
#include <vector>
#include <tuple>
#include <optional>

enum class TSMValueType { Float = 0, Boolean };

typedef struct TSMIndexBlock {
  uint64_t minTime;
  uint64_t maxTime;
  uint64_t offset;
  uint32_t size;
} TSMIndexBlock;

typedef struct TSMIndexEntry {
  std::string seriesId;
  TSMValueType seriesType;
  std::vector<TSMIndexBlock> indexBlocks;

  bool operator < (const TSMIndexEntry& str) const
  {
      return (seriesId > str.seriesId);
  }
} TSMIndexEntry;

class TSM {
private:
  std::string filename;
  std::unique_ptr<std::ifstream> tsmFile;
  size_t length;

  // TODO: Test using tsl::htrie_map to save memory
  std::unordered_map<std::string, TSMIndexEntry> index;

public:
  unsigned int seqNum;
  unsigned int tierNum;

  
  TSM(std::string _filename);
  void readIndex();
  template <class T>
  void readSeries(std::string seriesKey, uint64_t startTime, uint64_t endTime, QueryResult<T> &results);
  template <class T>
  void readBlock(const TSMIndexBlock &indexBlock, uint64_t startTime, uint64_t endTime, QueryResult<T> &results);
  std::optional<TSMValueType> getSeriesType(std::string seriesKey);

  template <class T>
  static constexpr TSMValueType getValueType(){
    if(std::is_same<T, double>::value){
      return TSMValueType::Float;
    } else if(std::is_same<T, bool>::value){
      return TSMValueType::Boolean;
    }
  };
};

#endif
