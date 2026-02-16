#ifndef QUERY_RESULT_H_INCLUDED
#define QUERY_RESULT_H_INCLUDED

#include <algorithm>
#include <functional>
#include <iostream>
#include <memory>
#include <queue>
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

  // Heap entry: references an iterator by index into blockIterState.
  // Stored values are denormalized from the iterator for fast comparison
  // without pointer chasing during heap operations.
  struct HeapEntry {
    uint64_t timestamp;
    uint64_t rank;
    size_t iterIndex;  // index into blockIterState vector

    // Min-heap by timestamp, then max by rank for tie-breaking.
    // std::priority_queue is a max-heap, so we invert the comparison:
    //   - higher timestamp => lower priority (comes later)
    //   - for equal timestamps, lower rank => lower priority (higher rank wins)
    bool operator>(const HeapEntry &other) const {
      if (timestamp != other.timestamp)
        return timestamp > other.timestamp;
      return rank < other.rank;
    }
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

  // Advance an iterator past all positions with timestamp <= targetTimestamp.
  // Returns true if the iterator still has data, false if exhausted.
  static bool advanceIteratorPast(TSMIterationState &state,
                                  uint64_t targetTimestamp,
                                  std::vector<TSMResult<T>> &tsmResults) {
    while (state.block != nullptr && state.currentTimestamp <= targetTimestamp) {
      state.blockOffset++;

      if (static_cast<size_t>(state.blockOffset) >= state.blockSize) {
        state.blockIdx++;
        state.blockOffset = 0;
        state.block =
            tsmResults[state.tsmResultIndex].getBlock(state.blockIdx);

        if (state.block == nullptr)
          return false;

        state.blockSize = state.block->size();
      }

      state.currentTimestamp = state.block->timestampAt(state.blockOffset);
    }
    return state.block != nullptr;
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

    // Initialize iteration state for each non-empty TSMResult
    std::vector<TSMIterationState> blockIterState;

    for (int i = 0; i < static_cast<int>(tsmResultSize); i++) {
      TSMBlock<T> *firstBlock = tsmResults[i].getBlock(0);
      if (firstBlock == nullptr)
        continue;

      TSMIterationState state = {i, firstBlock, 0, 0,
                                 firstBlock->size(), 0, tsmResults[i].rank};

      state.currentTimestamp = state.block->timestampAt(state.blockOffset);

      blockIterState.push_back(state);
    }

    if (blockIterState.empty())
      return;

    // Build min-heap: ordered by (timestamp ASC, rank DESC).
    // Using std::greater<> with our operator> gives min-heap behavior.
    std::priority_queue<HeapEntry, std::vector<HeapEntry>,
                        std::greater<HeapEntry>>
        heap;

    for (size_t i = 0; i < blockIterState.size(); i++) {
      heap.push({blockIterState[i].currentTimestamp,
                 blockIterState[i].rank, i});
    }

    // Temporary buffer for collecting all entries at the same timestamp.
    // Pre-allocated once to avoid repeated allocation inside the loop.
    std::vector<HeapEntry> sameTimestampEntries;
    sameTimestampEntries.reserve(blockIterState.size());

    while (!heap.empty()) {
      // The top of the heap has the smallest timestamp. Due to our comparator,
      // among entries with the same timestamp, the highest rank comes first.
      HeapEntry best = heap.top();
      heap.pop();

      const uint64_t currentTs = best.timestamp;

      // Collect all other heap entries that share the same timestamp.
      // We need to drain them so each timestamp is output exactly once.
      sameTimestampEntries.clear();

      while (!heap.empty() && heap.top().timestamp == currentTs) {
        sameTimestampEntries.push_back(heap.top());
        heap.pop();
      }

      // Output the winning point. 'best' already has the highest rank for
      // this timestamp because the heap orders by (timestamp ASC, rank DESC).
      auto &winnerState = blockIterState[best.iterIndex];
      timestamps.push_back(currentTs);
      values.push_back(winnerState.block->valueAt(winnerState.blockOffset));

      // Advance the winning iterator past currentTs and re-push if it has more data
      if (advanceIteratorPast(winnerState, currentTs, tsmResults)) {
        heap.push({winnerState.currentTimestamp, winnerState.rank,
                   best.iterIndex});
      }

      // Advance all other iterators that had the same timestamp (deduplication)
      for (auto &entry : sameTimestampEntries) {
        auto &state = blockIterState[entry.iterIndex];
        if (advanceIteratorPast(state, currentTs, tsmResults)) {
          heap.push({state.currentTimestamp, state.rank, entry.iterIndex});
        }
      }
    }
  }
};

using VariantQueryResult = std::variant<QueryResult<double>, QueryResult<bool>, QueryResult<std::string>>;

#endif
