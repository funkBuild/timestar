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

  // Single-source fast path: no merge or dedup needed.
  // Just concatenate all blocks' data directly into the output.
  void mergeSingleSource(TSMIterationState &state,
                         std::vector<TSMResult<T>> &tsmResults) {
    auto &result = tsmResults[state.tsmResultIndex];
    for (size_t b = 0; ; ++b) {
      TSMBlock<T> *block = result.getBlock(b);
      if (block == nullptr)
        break;
      timestamps.insert(timestamps.end(),
                        block->timestamps->begin(),
                        block->timestamps->end());
      values.insert(values.end(),
                    block->values->begin(),
                    block->values->end());
    }
  }

  // Two-way merge: direct comparison between two iterators, no heap overhead.
  // For each step, compare the two current timestamps and emit the smaller.
  // On equal timestamps, the higher-rank source wins (dedup).
  void mergeTwoWay(std::vector<TSMIterationState> &iters,
                   std::vector<TSMResult<T>> &tsmResults) {
    auto &s0 = iters[0];
    auto &s1 = iters[1];

    while (s0.block != nullptr && s1.block != nullptr) {
      const uint64_t ts0 = s0.currentTimestamp;
      const uint64_t ts1 = s1.currentTimestamp;

      if (ts0 < ts1) {
        // Source 0 has the smaller timestamp -- emit it and advance
        timestamps.push_back(ts0);
        values.push_back(s0.block->valueAt(s0.blockOffset));
        advanceIteratorPast(s0, ts0, tsmResults);
      } else if (ts1 < ts0) {
        // Source 1 has the smaller timestamp -- emit it and advance
        timestamps.push_back(ts1);
        values.push_back(s1.block->valueAt(s1.blockOffset));
        advanceIteratorPast(s1, ts1, tsmResults);
      } else {
        // Equal timestamps -- higher rank wins, advance both (dedup)
        if (s0.rank >= s1.rank) {
          timestamps.push_back(ts0);
          values.push_back(s0.block->valueAt(s0.blockOffset));
        } else {
          timestamps.push_back(ts1);
          values.push_back(s1.block->valueAt(s1.blockOffset));
        }
        advanceIteratorPast(s0, ts0, tsmResults);
        advanceIteratorPast(s1, ts1, tsmResults);
      }
    }

    // Drain whichever source still has data
    auto drainRemaining = [&](TSMIterationState &s) {
      while (s.block != nullptr) {
        timestamps.push_back(s.currentTimestamp);
        values.push_back(s.block->valueAt(s.blockOffset));
        const uint64_t ts = s.currentTimestamp;
        advanceIteratorPast(s, ts, tsmResults);
      }
    };

    drainRemaining(s0);
    drainRemaining(s1);
  }

  // N-way merge for small N (3-4 sources): linear scan to find minimum.
  // For N <= 4, a linear scan with branch-free comparison is faster than
  // any heap or tournament tree due to cache locality and low overhead.
  void mergeSmallN(std::vector<TSMIterationState> &iters,
                   const size_t numIters,
                   std::vector<TSMResult<T>> &tsmResults) {
    size_t activeCount = numIters;

    while (activeCount > 0) {
      // Find the source with minimum timestamp (highest rank breaks ties).
      // Linear scan over at most 4 elements.
      size_t bestIdx = SIZE_MAX;
      uint64_t bestTs = UINT64_MAX;
      uint64_t bestRank = 0;

      for (size_t i = 0; i < numIters; ++i) {
        if (iters[i].block == nullptr)
          continue;

        const uint64_t ts = iters[i].currentTimestamp;
        const uint64_t rk = iters[i].rank;

        if (ts < bestTs || (ts == bestTs && rk > bestRank)) {
          bestTs = ts;
          bestRank = rk;
          bestIdx = i;
        }
      }

      // Emit the winning point
      timestamps.push_back(bestTs);
      values.push_back(iters[bestIdx].block->valueAt(iters[bestIdx].blockOffset));

      // Advance ALL sources that share the same timestamp (dedup).
      for (size_t i = 0; i < numIters; ++i) {
        if (iters[i].block != nullptr && iters[i].currentTimestamp == bestTs) {
          if (!advanceIteratorPast(iters[i], bestTs, tsmResults)) {
            --activeCount;
          }
        }
      }
    }
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

    const size_t numSources = blockIterState.size();

    // Dispatch to specialized merge based on source count.
    // The common case is 1-4 TSM files per series; specialized paths
    // eliminate heap overhead for these cases.
    if (numSources == 1) {
      // Single source: direct copy, no merge or dedup needed.
      mergeSingleSource(blockIterState[0], tsmResults);
      return;
    }

    if (numSources == 2) {
      // Two-way merge: direct comparison, no heap.
      mergeTwoWay(blockIterState, tsmResults);
      return;
    }

    if (numSources <= 4) {
      // 3-4 way merge: linear scan for minimum, no heap.
      mergeSmallN(blockIterState, numSources, tsmResults);
      return;
    }

    // 5+ sources: heap-based k-way merge (general case).
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
