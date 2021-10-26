#ifndef __QUERY_RESULT_H_INCLUDED__
#define __QUERY_RESULT_H_INCLUDED__

#include <vector>
#include <stdint.h>
#include <variant>
#include <memory>
#include <algorithm>

template <class T>
class QueryResult {
private:

public:
  QueryResult() {
    timestamps = std::make_unique<std::vector<uint64_t>>();
    values = std::make_unique<std::vector<T>>();
  };

  uint64_t minTime = 0, maxTime = 0;

  std::unique_ptr<std::vector<uint64_t>> timestamps;
  std::unique_ptr<std::vector<T>> values;

  void extendCapacity(size_t extraSize){
    const size_t reserveSize = timestamps->size() + extraSize;
    timestamps->reserve(reserveSize);
    values->reserve(reserveSize);
  }

  void appendBlock(std::vector<uint64_t> &blockTimestamps, std::vector<T> &blockValues){
    std::copy(blockTimestamps.begin(), blockTimestamps.end(), std::back_inserter(*timestamps));
    std::copy(blockValues.begin(), blockValues.end(), std::back_inserter(*values));
  }

  void mergeQueryResult(QueryResult<T> &result){
    std::vector<uint64_t> *timestampsResultPtr = result.timestamps.get();
    std::vector<T> *valuesResultPtr = result.values.get();

    // Results to merge is empty
    if(timestampsResultPtr->size() == 0){
      return;
    }

    // Current results is empty
    if(timestamps->size() == 0){
      timestamps = std::move(result.timestamps);
      values = std::move(result.values);
      return;
    }
    
    // TODO: Fast path when incoming timestamps doesn't overlap with current timestamps

    const auto [minTime, maxTime] = std::minmax_element(begin(*timestampsResultPtr), end(*timestampsResultPtr));

    std::unique_ptr<std::vector<uint64_t>> mergedTimestamps = std::make_unique<std::vector<uint64_t>>();
    std::unique_ptr<std::vector<T>> mergedValues = std::make_unique<std::vector<T>>();

    const size_t mergedMaxSize = timestamps->size() + timestampsResultPtr->size();
    mergedTimestamps->reserve(mergedMaxSize);
    mergedValues->reserve(mergedMaxSize);


    unsigned int aOffset = 0, bOffset = 0;
    std::vector<uint64_t> *timestampsPtr = timestamps.get();
    std::vector<T> *valuesPtr = values.get();

    while(aOffset < timestampsPtr->size() && bOffset < timestampsResultPtr->size()){
      int64_t diff = timestampsPtr->at(aOffset) - timestampsResultPtr->at(bOffset);

      if(diff == 0){
        // Prefer the incoming data (should always be the newest insert)
        mergedTimestamps->push_back(timestampsResultPtr->at(bOffset));
        mergedValues->push_back(valuesResultPtr->at(bOffset));

        aOffset++;
        bOffset++;
      } else if(diff > 0){
        // blockTimestamps has the smaller timestamp
        mergedTimestamps->push_back(timestampsResultPtr->at(bOffset));
        mergedValues->push_back(valuesResultPtr->at(bOffset));
        bOffset++;
      } else {
        mergedTimestamps->push_back(timestampsPtr->at(aOffset));
        mergedValues->push_back(valuesPtr->at(aOffset));
        aOffset++;
      }
    }

    while(aOffset < timestamps->size()){
      mergedTimestamps->push_back(timestampsPtr->at(aOffset));
      mergedValues->push_back(valuesPtr->at(aOffset));
      aOffset++;
    }

    while(bOffset < timestampsResultPtr->size()){
      mergedTimestamps->push_back(timestampsResultPtr->at(bOffset));
      mergedValues->push_back(valuesResultPtr->at(bOffset));
      bOffset++;
    }

    timestamps = std::move(mergedTimestamps);
    values = std::move(mergedValues);
  }
};

using VariantQueryResult = std::variant<QueryResult<double>, QueryResult<bool>>;

#endif
