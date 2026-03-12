#ifndef COMPACTION_PIPELINE_H_INCLUDED
#define COMPACTION_PIPELINE_H_INCLUDED

#include "tsm_compactor.hpp"

#include <memory>
#include <queue>
#include <seastar/core/future.hh>
#include <seastar/core/queue.hh>

// Phase 2.1: Series prefetch manager for pipelined compaction
// Overlaps I/O (reading series N+1) with computation (processing series N)
template <typename T>
class SeriesPrefetchManager {
private:
    struct PrefetchedSeries {
        SeriesId128 seriesId;
        std::unique_ptr<TSMMergeIterator<T>> iterator;
        bool initialized = false;
    };

    std::vector<seastar::shared_ptr<TSM>> files;
    std::vector<SeriesId128> seriesQueue;  // Series to process
    size_t currentIndex = 0;

    // Prefetch depth - how many series to prefetch ahead
    size_t prefetchDepth;

    // Active prefetch futures
    std::queue<seastar::future<PrefetchedSeries>> prefetchQueue;

    // Memory tracking (Phase 2.3)
    size_t maxMemoryBytes;
    size_t currentMemoryUsage{0};

public:
    SeriesPrefetchManager(const std::vector<seastar::shared_ptr<TSM>>& tsmFiles, const std::vector<SeriesId128>& series,
                          size_t depth = 2,
                          size_t maxMemory = 256 * 1024 * 1024  // 256MB default
                          )
        : files(tsmFiles), seriesQueue(series), prefetchDepth(depth), maxMemoryBytes(maxMemory) {}

    // Phase 2.1: Start prefetching pipeline
    seastar::future<> init() {
        // Start prefetching initial batch
        for (size_t i = 0; i < std::min(prefetchDepth, seriesQueue.size()); i++) {
            startPrefetch(currentIndex + i);
        }
        co_return;
    }

    // Phase 2.1: Get next prefetched series (await if not ready)
    seastar::future<std::unique_ptr<TSMMergeIterator<T>>> getNext() {
        if (currentIndex >= seriesQueue.size()) {
            co_return nullptr;  // No more series
        }

        // Get the prefetched series (or wait for it to complete)
        PrefetchedSeries prefetched;
        if (!prefetchQueue.empty()) {
            prefetched = co_await std::move(prefetchQueue.front());
            prefetchQueue.pop();
        } else {
            // Fallback: no prefetch available, create synchronously
            prefetched.seriesId = seriesQueue[currentIndex];
            prefetched.iterator = std::make_unique<TSMMergeIterator<T>>(prefetched.seriesId, files);
            co_await prefetched.iterator->init();
        }

        currentIndex++;

        // Start prefetching next series in background
        if (currentIndex + prefetchDepth - 1 < seriesQueue.size()) {
            startPrefetch(currentIndex + prefetchDepth - 1);
        }

        co_return std::move(prefetched.iterator);
    }

    // Check if more series are available
    bool hasNext() const { return currentIndex < seriesQueue.size(); }

    // Get current prefetch queue size (for monitoring)
    size_t getPrefetchQueueSize() const { return prefetchQueue.size(); }

private:
    // Phase 2.1: Start async prefetch of a series (non-blocking)
    void startPrefetch(size_t index) {
        if (index >= seriesQueue.size()) {
            return;
        }

        // Launch async initialization
        auto future = [this, index]() -> seastar::future<PrefetchedSeries> {
            PrefetchedSeries result;
            result.seriesId = seriesQueue[index];
            result.iterator = std::make_unique<TSMMergeIterator<T>>(result.seriesId, files);

            co_await result.iterator->init();
            result.initialized = true;

            co_return result;
        }();

        prefetchQueue.push(std::move(future));
    }
};

#endif  // COMPACTION_PIPELINE_H_INCLUDED
