#ifndef TSM_RESULT_H_INCLUDED
#define TSM_RESULT_H_INCLUDED

#include <vector>
#include <stdint.h>
#include <variant>
#include <memory>
#include <algorithm>
#include <iostream>

#include "util.hpp"

template <class T>
class TSMBlock {
private:

public:
  std::unique_ptr<std::vector<uint64_t>> timestamps;
  std::unique_ptr<std::vector<T>> values;

  TSMBlock(size_t initialSize) {
    timestamps = std::make_unique<std::vector<uint64_t>>();
    values = std::make_unique<std::vector<T>>();

    timestamps->reserve(initialSize);
    values->reserve(initialSize);
  };

  uint64_t startTime(){
    return timestamps->front();
  }

  uint64_t endTime(){
    return timestamps->back();
  }

  size_t size(){
    return timestamps->size();
  }

  uint64_t timestampAt(unsigned int idx){
    return timestamps->at(idx);
  }

  T valueAt(unsigned int idx){
    return values->at(idx);
  }
};

template <class T>
class TSMResult {
private:

public:
  uint64_t rank;
  std::vector<std::unique_ptr<TSMBlock<T>>> blocks;

  TSMResult(uint64_t _rank) : rank(_rank) {
    blocks.reserve(100);
  };

  TSMBlock<T>* getBlock(int idx){
    if(idx >= blocks.size())
      return nullptr;

    return blocks[idx].get();
  }

  bool empty(){
    return blocks.size() == 0;
  }

  void appendBlock(std::unique_ptr<TSMBlock<T>> &block){
    blocks.push_back(std::move(block));
  }

  void sort(){
    std::sort(blocks.begin(), blocks.end(), [](const auto& lhs, const auto& rhs){
      return lhs->startTime() < rhs->startTime();
    });
  }

  void print(){
    // Debug output - consider using logger if needed
    // for(auto& block: blocks){
    //   std::cout << "startTime=" << block->startTime() << " endTime=" << block->endTime() << std::endl;
    // }
  }
  
  // Get all timestamps and values from all blocks
  std::pair<std::vector<uint64_t>, std::vector<T>> getAllData(){
    std::vector<uint64_t> allTimestamps;
    std::vector<T> allValues;
    
    // Calculate total size
    size_t totalSize = 0;
    for(auto& block: blocks){
      totalSize += block->size();
    }
    
    allTimestamps.reserve(totalSize);
    allValues.reserve(totalSize);
    
    // Collect data from all blocks
    for(auto& block: blocks){
      allTimestamps.insert(allTimestamps.end(), block->timestamps->begin(), block->timestamps->end());
      allValues.insert(allValues.end(), block->values->begin(), block->values->end());
    }
    
    return {allTimestamps, allValues};
  }
};

#endif
