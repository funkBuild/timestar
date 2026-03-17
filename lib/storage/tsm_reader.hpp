#pragma once

#include "tsm.hpp"

#include <seastar/core/shared_ptr.hh>

// RAII wrapper for TSM file access.
// Seastar's shard-per-core model guarantees no cross-shard sharing of TSM
// objects, so reference counting is unnecessary. This is a thin passthrough.
class TSMReader {
private:
    seastar::shared_ptr<TSM> tsm;

public:
    explicit TSMReader(seastar::shared_ptr<TSM> file) : tsm(std::move(file)) {}

    ~TSMReader() = default;

    // Delete copy constructor and assignment
    TSMReader(const TSMReader&) = delete;
    TSMReader& operator=(const TSMReader&) = delete;

    // Move constructor
    TSMReader(TSMReader&& other) noexcept : tsm(std::move(other.tsm)) { other.tsm = nullptr; }

    // Move assignment
    TSMReader& operator=(TSMReader&& other) noexcept {
        if (this != &other) {
            tsm = std::move(other.tsm);
            other.tsm = nullptr;
        }
        return *this;
    }

    // Access the underlying TSM file
    TSM* operator->() const { return tsm.get(); }
    TSM& operator*() const { return *tsm; }

    // Check if valid
    explicit operator bool() const { return tsm != nullptr; }

    // Get the underlying pointer
    TSM* get() const { return tsm.get(); }

    // Read series with automatic reference management
    template <class T>
    seastar::future<TSMResult<T>> readSeries(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime) {
        TSMResult<T> results(tsm->rankAsInteger());
        co_await tsm->readSeries<T>(seriesId, startTime, endTime, results);
        co_return results;
    }
};

// Helper to create a reader
inline TSMReader makeTSMReader(seastar::shared_ptr<TSM> file) {
    return TSMReader(file);
}
