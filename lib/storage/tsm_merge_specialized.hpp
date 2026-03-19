#pragma once

#include "series_id.hpp"
#include "tsm.hpp"
#include "tsm_block_iterator.hpp"

#include <limits>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <vector>

// Phase 5.1: Specialized N-way merge iterators for optimal performance
// Eliminates priority_queue overhead for common merge cases (2-4 files)

// Phase 5.1: Two-way merge iterator - direct comparison, no heap
template <typename T>
class TwoWayMergeIterator {
private:
    struct Source {
        seastar::shared_ptr<TSM> file;
        seastar::lw_shared_ptr<TSMBlockIterator<T>> blockIterator;  // shard-local, non-atomic refcount
        TSMBlock<T>* currentBlock = nullptr;
        size_t pointIndex = 0;
        bool exhausted = false;

        Source(seastar::shared_ptr<TSM> f, const SeriesId128& seriesId) : file(f) {
            blockIterator = seastar::make_lw_shared<TSMBlockIterator<T>>(f, seriesId, 0, UINT64_MAX);
        }

        uint64_t currentTimestamp() const {
            if (exhausted || !currentBlock || pointIndex >= currentBlock->timestamps.size()) {
                return UINT64_MAX;
            }
            return currentBlock->timestamps[pointIndex];
        }

        T currentValue() const { return currentBlock->values[pointIndex]; }

        seastar::future<> advance() {
            pointIndex++;
            if (currentBlock && pointIndex >= currentBlock->timestamps.size()) {
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
    TwoWayMergeIterator(const SeriesId128& series, const std::vector<seastar::shared_ptr<TSM>>& files)
        : sources{Source(files[0], series), Source(files[1], series)}, seriesId(series) {}

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

    bool hasNext() const { return !sources[0].exhausted || !sources[1].exhausted; }

    const SeriesId128& getSeriesId() const { return seriesId; }

    // Direct comparison between two sources (no heap).
    // On equal timestamps, prefer source 1 (newer file) and advance both
    // sources to deduplicate.
    seastar::future<std::pair<uint64_t, T>> next() {
        uint64_t ts0 = sources[0].currentTimestamp();
        uint64_t ts1 = sources[1].currentTimestamp();

        uint64_t timestamp;
        T value;

        if (ts0 < ts1) {
            timestamp = ts0;
            value = sources[0].currentValue();
            co_await sources[0].advance();
        } else if (ts1 < ts0) {
            timestamp = ts1;
            value = sources[1].currentValue();
            co_await sources[1].advance();
        } else {
            // Equal timestamps — take newer file's value, advance both
            timestamp = ts1;
            value = sources[1].currentValue();
            co_await sources[0].advance();
            co_await sources[1].advance();
        }

        co_return std::make_pair(timestamp, std::move(value));
    }

    seastar::future<std::vector<std::pair<uint64_t, T>>> nextBatch(size_t maxPoints = 1000) {
        std::vector<std::pair<uint64_t, T>> batch;
        batch.reserve(maxPoints);

        while (hasNext() && batch.size() < maxPoints) {
            auto point = co_await next();
            batch.push_back(std::move(point));
        }

        co_return batch;
    }
};

// Phase 5.1: Four-way merge iterator - tournament-style comparison tree
template <typename T>
class FourWayMergeIterator {
private:
    struct Source {
        seastar::shared_ptr<TSM> file;
        seastar::lw_shared_ptr<TSMBlockIterator<T>> blockIterator;  // shard-local, non-atomic refcount
        TSMBlock<T>* currentBlock = nullptr;
        size_t pointIndex = 0;
        bool exhausted = false;
        uint64_t rank = 0;  // File rank for tie-breaking

        Source() { exhausted = true; }
        Source(seastar::shared_ptr<TSM> f, const SeriesId128& seriesId) : file(f), rank(f->rankAsInteger()) {
            blockIterator = seastar::make_lw_shared<TSMBlockIterator<T>>(f, seriesId, 0, UINT64_MAX);
        }

        uint64_t currentTimestamp() const {
            if (exhausted || !currentBlock || pointIndex >= currentBlock->timestamps.size()) {
                return UINT64_MAX;
            }
            return currentBlock->timestamps[pointIndex];
        }

        T currentValue() const { return currentBlock->values[pointIndex]; }

        seastar::future<> advance() {
            pointIndex++;
            if (currentBlock && pointIndex >= currentBlock->timestamps.size()) {
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
    FourWayMergeIterator(const SeriesId128& series, const std::vector<seastar::shared_ptr<TSM>>& files)
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
            if (!src.exhausted)
                return true;
        }
        return false;
    }

    const SeriesId128& getSeriesId() const { return seriesId; }

    // Tournament-style 4-way comparison. On equal timestamps, the source with
    // the highest rank (newest file) wins and ALL sources at that timestamp
    // are advanced to deduplicate.
    seastar::future<std::pair<uint64_t, T>> next() {
        uint64_t ts0 = sources[0].currentTimestamp();
        uint64_t ts1 = sources[1].currentTimestamp();
        uint64_t ts2 = sources[2].currentTimestamp();
        uint64_t ts3 = sources[3].currentTimestamp();

        // Find minimum using tournament tree (3 comparisons)
        size_t winner01 = (ts0 < ts1) ? 0 : (ts1 < ts0) ? 1 : (sources[1].rank > sources[0].rank ? 1 : 0);
        size_t winner23 = (ts2 < ts3) ? 2 : (ts3 < ts2) ? 3 : (sources[3].rank > sources[2].rank ? 3 : 2);

        uint64_t tsWinner01 = sources[winner01].currentTimestamp();
        uint64_t tsWinner23 = sources[winner23].currentTimestamp();

        size_t chosenIdx = (tsWinner01 < tsWinner23) ? winner01
                           : (tsWinner23 < tsWinner01)
                               ? winner23
                               : (sources[winner23].rank > sources[winner01].rank ? winner23 : winner01);

        uint64_t timestamp = sources[chosenIdx].currentTimestamp();
        T value = sources[chosenIdx].currentValue();

        // Advance ALL sources at this timestamp to deduplicate
        for (auto& src : sources) {
            if (!src.exhausted && src.currentTimestamp() == timestamp) {
                co_await src.advance();
            }
        }

        co_return std::make_pair(timestamp, std::move(value));
    }

    seastar::future<std::vector<std::pair<uint64_t, T>>> nextBatch(size_t maxPoints = 1000) {
        std::vector<std::pair<uint64_t, T>> batch;
        batch.reserve(maxPoints);

        while (hasNext() && batch.size() < maxPoints) {
            // next() already deduplicates equal timestamps
            auto point = co_await next();
            batch.push_back(std::move(point));
        }

        co_return batch;
    }
};
