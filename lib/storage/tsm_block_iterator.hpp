#pragma once

#include "tsm.hpp"
#include "tsm_result.hpp"

#include <optional>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <vector>

// Phase 1.2: Streaming block iterator for lazy/on-demand block loading
// This replaces the pattern of loading entire series upfront with
// loading one block at a time as needed
template <typename T>
class TSMBlockIterator {
private:
    seastar::shared_ptr<TSM> file;
    SeriesId128 seriesId;
    std::vector<TSMIndexBlock> blocks;  // Index metadata only (lightweight)
    uint64_t startTime;
    uint64_t endTime;

    size_t currentBlockIdx = 0;
    std::unique_ptr<TSMBlock<T>> currentBlock;

    // Prefetch support - initiate async load without awaiting
    std::optional<seastar::future<std::unique_ptr<TSMBlock<T>>>> prefetchFuture;

public:
    // Constructor - takes file and series ID
    TSMBlockIterator(seastar::shared_ptr<TSM> f, const SeriesId128& id, uint64_t start = 0, uint64_t end = UINT64_MAX)
        : file(f), seriesId(id), startTime(start), endTime(end) {}

    ~TSMBlockIterator() {
        // If a prefetch is outstanding, we must not let Seastar assert on
        // "future destroyed without being awaited". Detach it with
        // handle_exception to consume the result silently.
        if (prefetchFuture.has_value()) {
            std::move(prefetchFuture.value()).then_wrapped([](auto f) {
                try { f.get(); } catch (...) {}
            });
            prefetchFuture.reset();
        }
    }

    // Non-copyable, movable
    TSMBlockIterator(const TSMBlockIterator&) = delete;
    TSMBlockIterator& operator=(const TSMBlockIterator&) = delete;
    TSMBlockIterator(TSMBlockIterator&&) = default;
    TSMBlockIterator& operator=(TSMBlockIterator&&) = default;

    // Phase 1.2: Initialize with just the index metadata (no I/O yet)
    seastar::future<> init() {
        // Get index blocks without reading data
        blocks = file->getSeriesBlocks(seriesId);

        // Filter blocks by time range
        auto it = std::remove_if(blocks.begin(), blocks.end(), [this](const TSMIndexBlock& block) {
            return block.minTime > endTime || block.maxTime < startTime;
        });
        blocks.erase(it, blocks.end());

        // Prefetch first block if available
        if (!blocks.empty()) {
            prefetchNext();
        }

        co_return;
    }

    // Phase 1.2: Check if more blocks are available
    bool hasNext() const { return currentBlockIdx < blocks.size(); }

    // Phase 1.2: Get next block (on-demand loading)
    seastar::future<TSMBlock<T>*> nextBlock() {
        if (!hasNext()) {
            co_return nullptr;
        }

        // If we have a prefetch in progress, await it
        if (prefetchFuture.has_value()) {
            currentBlock = co_await std::move(prefetchFuture.value());
            prefetchFuture.reset();
        } else {
            // No prefetch, load synchronously
            const auto& block = blocks[currentBlockIdx];
            currentBlock = co_await file->template readSingleBlock<T>(block, startTime, endTime);
        }

        currentBlockIdx++;

        // Prefetch next block in background
        if (hasNext()) {
            prefetchNext();
        }

        co_return currentBlock.get();
    }

    // Phase 1.2: Initiate async prefetch of next block (doesn't block)
    void prefetchNext() {
        if (currentBlockIdx < blocks.size()) {
            const auto& block = blocks[currentBlockIdx];
            prefetchFuture = file->template readSingleBlock<T>(block, startTime, endTime);
        }
    }

    // Get current block index (for debugging/monitoring)
    size_t getCurrentBlockIndex() const { return currentBlockIdx; }

    // Get total number of blocks
    size_t getTotalBlocks() const { return blocks.size(); }

    // Get file rank (for merge priority)
    uint64_t getFileRank() const { return file->rankAsInteger(); }
};
