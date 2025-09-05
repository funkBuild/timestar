#ifndef __QUERY_RESULT_H_INCLUDED__
#define __QUERY_RESULT_H_INCLUDED__

#include <algorithm>
#include <iostream>
#include <memory>
#include <stdint.h>
#include <variant>
#include <vector>

#include "tsm_result.hpp"
#include "util.hpp"
#include <chrono>


typedef std::chrono::high_resolution_clock Clock;

template <class T> class QueryResult {
private:
  struct TSMIterationState {
    TSMBlock<T> *block;
    int blockIdx;
    int blockOffset;
    size_t blockSize;
    uint64_t currentTimestamp;
  };

public:
  std::vector<uint64_t> timestamps;
  std::vector<T> values;

  QueryResult() {
    timestamps.reserve(10000);
    values.reserve(10000);
  };

  static QueryResult fromTsmResults(std::vector<TSMResult<T>> &tsmResults) {
    QueryResult<T> results;

    auto start_time = Clock::now();
    results.mergeTsmResults(tsmResults);

    auto end_time = Clock::now();

    uint64_t time_diff = std::chrono::duration_cast<std::chrono::microseconds>(
                             end_time - start_time)
                             .count();

    // Debug output - consider using logger if needed
    // std::cout << "Merge time: " << time_diff / 1000.0 << " milliseconds"
    //           << std::endl;

    return results;
  }

  void mergeTsmResults(std::vector<TSMResult<T>> &tsmResults) {
    const unsigned int tsmResultSize = tsmResults.size();

    std::vector<TSMIterationState> blockIterState;

    for (int i = 0; i < tsmResultSize; i++) {
      TSMIterationState state = {tsmResults[i].getBlock(0), 0, 0,
                                 tsmResults[i].getBlock(0)->size(), 0};

      if (state.block == nullptr)
        continue;

      state.currentTimestamp = state.block->timestampAt(state.blockOffset);

      blockIterState.push_back(state);
    }

    while (true) {
      uint64_t minTimestamp = std::numeric_limits<uint64_t>::max();
      TSMIterationState *minState = nullptr;

      // Get the block with the lowest timestamp
      for (int i = 0; i < tsmResultSize; i++) {
        TSMIterationState *state = &blockIterState[i];

        if (state->block == nullptr)
          continue;

        if (state->currentTimestamp < minTimestamp) {
          minTimestamp = state->currentTimestamp;
          minState = state;
        }
      }
      // Bail if there's no more blocks to consume
      if (minState == nullptr)
        break;

      timestamps.push_back(minTimestamp);
      values.push_back(minState->block->valueAt(minState->blockOffset));

      for (int i = 0; i < tsmResultSize; i++) {
        TSMIterationState *state = &blockIterState[i];

        if (state->block == nullptr)
          continue;

        while (state->currentTimestamp <= minTimestamp) {
          state->blockOffset++;

          if (state->blockOffset >= state->blockSize) {
            state->blockIdx++;
            state->blockOffset = 0;
            state->block = tsmResults[i].getBlock(state->blockIdx);

            if (state->block == nullptr)
              break;

            state->blockSize = state->block->size();
          }

          state->currentTimestamp =
              state->block->timestampAt(state->blockOffset);
        }
      }
    }
  }
};

using VariantQueryResult = std::variant<QueryResult<double>, QueryResult<bool>, QueryResult<std::string>>;

#endif
