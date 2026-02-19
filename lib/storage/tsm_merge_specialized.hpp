#ifndef TSM_MERGE_SPECIALIZED_H_INCLUDED
#define TSM_MERGE_SPECIALIZED_H_INCLUDED

#include "tsm_block_iterator.hpp"
#include "tsm.hpp"
#include "series_id.hpp"
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <vector>
#include <limits>

// Phase 5.1: Specialized N-way merge iterators for optimal performance
// Eliminates priority_queue overhead for common merge cases (2-4 files)

// Phase 5.1: Two-way merge iterator - direct comparison, no heap
template<typename T>
class TwoWayMergeIterator {
private:
    struct Source {
        seastar::shared_ptr<TSM> file;
        seastar::lw_shared_ptr<TSMBlockIterator<T>> blockIterator;  // shard-local, non-atomic refcount
        TSMBlock<T>* currentBlock = nullptr;
        size_t pointIndex = 0;
        bool exhausted = false;

        Source(seastar::shared_ptr<TSM> f, const SeriesId128& seriesId)
            : file(f) {
            blockIterator = seastar::make_lw_shared<TSMBlockIterator<T>>(f, seriesId, 0, UINT64_MAX);
        }

        uint64_t currentTimestamp() const {
            if (exhausted || !currentBlock || pointIndex >= currentBlock->timestamps->size()) {
                return UINT64_MAX;
            }
            return (*currentBlock->timestamps)[pointIndex];
        }

        T currentValue() const {
            return (*currentBlock->values)[pointIndex];
        }

        seastar::future<> advance() {
            pointIndex++;
            if (currentBlock && pointIndex >= currentBlock->timestamps->size()) {
                if (blockIterator->hasNext()) {
                    currentBlock = co_await blockIterator->nextBlock();
                    pointIndex = 0;
                } else {
                    exhausted = true;
                    currentBlock = nullptr;
                }
            }
        }
    };

    Source sources[2];
    SeriesId128 seriesId;
    bool initialized = false;

public:
    TwoWayMergeIterator(const SeriesId128& series,
                        const std::vector<seastar::shared_ptr<TSM>>& files)
        : sources{Source(files[0], series), Source(files[1], series)},
          seriesId(series) {}

    seastar::future<> init() {
        for (auto& src : sources) {
            co_await src.blockIterator->init();
            if (src.blockIterator->hasNext()) {
                src.currentBlock = co_await src.blockIterator->nextBlock();
            } else {
                src.exhausted = true;
            }
        }
        initialized = true;
    }

    bool hasNext() const {
        return !sources[0].exhausted || !sources[1].exhausted;
    }

    const SeriesId128& getSeriesId() const {
        return seriesId;
    }

    // Phase 5.1: Direct comparison between two sources (no heap)
    seastar::future<std::pair<uint64_t, T>> next() {
        uint64_t ts0 = sources[0].currentTimestamp();
        uint64_t ts1 = sources[1].currentTimestamp();

        // Direct comparison - choose smaller timestamp
        // On equal timestamps, prefer source 1 (newer file, higher rank)
        size_t chosenIdx;
        if (ts0 < ts1) {
            chosenIdx = 0;
        } else if (ts1 < ts0) {
            chosenIdx = 1;
        } else {
            // Equal timestamps - prefer newer file (index 1)
            chosenIdx = 1;
        }

        auto& chosen = sources[chosenIdx];
        uint64_t timestamp = chosen.currentTimestamp();
        T value = chosen.currentValue();

        co_await chosen.advance();

        co_return std::make_pair(timestamp, value);
    }

    seastar::future<std::vector<std::pair<uint64_t, T>>> nextBatch(size_t maxPoints = 1000) {
        std::vector<std::pair<uint64_t, T>> batch;
        batch.reserve(maxPoints);

        while (hasNext() && batch.size() < maxPoints) {
            auto [ts, val] = co_await next();

            // Skip duplicates from the other source
            uint64_t otherTs = sources[0].exhausted ? UINT64_MAX :
                              (sources[1].exhausted ? UINT64_MAX :
                               (sources[0].currentTimestamp() == ts ? sources[1].currentTimestamp() : sources[0].currentTimestamp()));

            while (hasNext() && otherTs == ts) {
                // Advance the source with same timestamp
                if (!sources[0].exhausted && sources[0].currentTimestamp() == ts) {
                    co_await sources[0].advance();
                }
                if (!sources[1].exhausted && sources[1].currentTimestamp() == ts) {
                    co_await sources[1].advance();
                }
                otherTs = sources[0].exhausted ? UINT64_MAX :
                         (sources[1].exhausted ? UINT64_MAX :
                          std::min(sources[0].currentTimestamp(), sources[1].currentTimestamp()));
            }

            batch.push_back({ts, val});
        }

        co_return batch;
    }
};

// Phase 5.1: Four-way merge iterator - tournament-style comparison tree
template<typename T>
class FourWayMergeIterator {
private:
    struct Source {
        seastar::shared_ptr<TSM> file;
        seastar::lw_shared_ptr<TSMBlockIterator<T>> blockIterator;  // shard-local, non-atomic refcount
        TSMBlock<T>* currentBlock = nullptr;
        size_t pointIndex = 0;
        bool exhausted = false;
        uint64_t rank = 0;  // File rank for tie-breaking

        Source() = default;
        Source(seastar::shared_ptr<TSM> f, const SeriesId128& seriesId)
            : file(f), rank(f->rankAsInteger()) {
            blockIterator = seastar::make_lw_shared<TSMBlockIterator<T>>(f, seriesId, 0, UINT64_MAX);
        }

        uint64_t currentTimestamp() const {
            if (exhausted || !currentBlock || pointIndex >= currentBlock->timestamps->size()) {
                return UINT64_MAX;
            }
            return (*currentBlock->timestamps)[pointIndex];
        }

        T currentValue() const {
            return (*currentBlock->values)[pointIndex];
        }

        seastar::future<> advance() {
            pointIndex++;
            if (currentBlock && pointIndex >= currentBlock->timestamps->size()) {
                if (blockIterator->hasNext()) {
                    currentBlock = co_await blockIterator->nextBlock();
                    pointIndex = 0;
                } else {
                    exhausted = true;
                    currentBlock = nullptr;
                }
            }
        }
    };

    Source sources[4];
    SeriesId128 seriesId;
    bool initialized = false;

public:
    FourWayMergeIterator(const SeriesId128& series,
                         const std::vector<seastar::shared_ptr<TSM>>& files)
        : seriesId(series) {
        for (size_t i = 0; i < 4 && i < files.size(); ++i) {
            sources[i] = Source(files[i], series);
        }
    }

    seastar::future<> init() {
        for (auto& src : sources) {
            if (src.blockIterator) {
                co_await src.blockIterator->init();
                if (src.blockIterator->hasNext()) {
                    src.currentBlock = co_await src.blockIterator->nextBlock();
                } else {
                    src.exhausted = true;
                }
            } else {
                src.exhausted = true;
            }
        }
        initialized = true;
    }

    bool hasNext() const {
        for (const auto& src : sources) {
            if (!src.exhausted) return true;
        }
        return false;
    }

    const SeriesId128& getSeriesId() const {
        return seriesId;
    }

    // Phase 5.1: Tournament-style 4-way comparison (3 comparisons)
    seastar::future<std::pair<uint64_t, T>> next() {
        // Tournament tree: compare pairs, then winners
        // Round 1: (0 vs 1) and (2 vs 3)
        uint64_t ts0 = sources[0].currentTimestamp();
        uint64_t ts1 = sources[1].currentTimestamp();
        uint64_t ts2 = sources[2].currentTimestamp();
        uint64_t ts3 = sources[3].currentTimestamp();

        // Find minimum using 3 comparisons (optimal for 4 elements)
        size_t winner01 = (ts0 < ts1) ? 0 : (ts1 < ts0) ? 1 : (sources[1].rank > sources[0].rank ? 1 : 0);
        size_t winner23 = (ts2 < ts3) ? 2 : (ts3 < ts2) ? 3 : (sources[3].rank > sources[2].rank ? 3 : 2);

        uint64_t tsWinner01 = sources[winner01].currentTimestamp();
        uint64_t tsWinner23 = sources[winner23].currentTimestamp();

        size_t chosenIdx = (tsWinner01 < tsWinner23) ? winner01 :
                          (tsWinner23 < tsWinner01) ? winner23 :
                          (sources[winner23].rank > sources[winner01].rank ? winner23 : winner01);

        auto& chosen = sources[chosenIdx];
        uint64_t timestamp = chosen.currentTimestamp();
        T value = chosen.currentValue();

        co_await chosen.advance();

        co_return std::make_pair(timestamp, value);
    }

    seastar::future<std::vector<std::pair<uint64_t, T>>> nextBatch(size_t maxPoints = 1000) {
        std::vector<std::pair<uint64_t, T>> batch;
        batch.reserve(maxPoints);

        while (hasNext() && batch.size() < maxPoints) {
            auto [timestamp, value] = co_await next();

            // Dedup: advance all sources at same timestamp
            while (hasNext()) {
                bool foundDup = false;
                for (auto& src : sources) {
                    if (!src.exhausted && src.currentTimestamp() == timestamp) {
                        co_await src.advance();
                        foundDup = true;
                    }
                }
                if (!foundDup) break;
            }

            batch.push_back({timestamp, value});
        }

        co_return batch;
    }
};

#endif // TSM_MERGE_SPECIALIZED_H_INCLUDED
