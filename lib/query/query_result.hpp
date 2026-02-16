#ifndef QUERY_RESULT_H_INCLUDED
#define QUERY_RESULT_H_INCLUDED

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
    int tsmResultIndex;
    TSMBlock<T> *block;
    int blockIdx;
    int blockOffset;
    size_t blockSize;
    uint64_t currentTimestamp;
    uint64_t rank;
  };

public:
  std::vector<uint64_t> timestamps;
  std::vector<T> values;

  QueryResult() = default;

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

    // Pre-calculate total points for efficient reservation
    size_t totalPoints = 0;
    for (unsigned int i = 0; i < tsmResultSize; i++) {
      for (size_t b = 0; tsmResults[i].getBlock(b) != nullptr; ++b) {
        totalPoints += tsmResults[i].getBlock(b)->size();
      }
    }
    timestamps.reserve(totalPoints);
    values.reserve(totalPoints);

    std::vector<TSMIterationState> blockIterState;

    for (int i = 0; i < tsmResultSize; i++) {
      TSMBlock<T> *firstBlock = tsmResults[i].getBlock(0);
      if (firstBlock == nullptr)
        continue;

      TSMIterationState state = {i, firstBlock, 0, 0,
                                 firstBlock->size(), 0, tsmResults[i].rank};

      state.currentTimestamp = state.block->timestampAt(state.blockOffset);

      blockIterState.push_back(state);
    }

    const size_t iterStateSize = blockIterState.size();

    while (true) {
      uint64_t minTimestamp = std::numeric_limits<uint64_t>::max();
      uint64_t minRank = 0;
      TSMIterationState *minState = nullptr;

      // Get the block with the lowest timestamp, preferring higher rank
      // (more recent TSM file) when timestamps are equal
      for (size_t i = 0; i < iterStateSize; i++) {
        TSMIterationState *state = &blockIterState[i];

        if (state->block == nullptr)
          continue;

        if (state->currentTimestamp < minTimestamp ||
            (state->currentTimestamp == minTimestamp && state->rank > minRank)) {
          minTimestamp = state->currentTimestamp;
          minRank = state->rank;
          minState = state;
        }
      }
      // Bail if there's no more blocks to consume
      if (minState == nullptr)
        break;

      timestamps.push_back(minTimestamp);
      values.push_back(minState->block->valueAt(minState->blockOffset));

      for (size_t i = 0; i < iterStateSize; i++) {
        TSMIterationState *state = &blockIterState[i];

        if (state->block == nullptr)
          continue;

        while (state->currentTimestamp <= minTimestamp) {
          state->blockOffset++;

          if (state->blockOffset >= state->blockSize) {
            state->blockIdx++;
            state->blockOffset = 0;
            state->block = tsmResults[state->tsmResultIndex].getBlock(state->blockIdx);

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
