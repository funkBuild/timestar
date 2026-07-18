#pragma once

#include "memory_store.hpp"
#include "tsm_file_manager.hpp"

#include <atomic>
#include <cstdint>
#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>
#include <stdexcept>
#include <vector>

class Engine;

namespace timestar {
// Thrown when a single insert/batch cannot fit in a WAL segment even after a
// rollover.  The HTTP layer maps this to 413 Payload Too Large (a client
// error), not a generic 500.
class InsertTooLargeException : public std::length_error {
public:
    explicit InsertTooLargeException(const std::string& what) : std::length_error(what) {}
};
}  // namespace timestar

// Pairs a raw pointer to in-memory series data with the shared_ptr that
// keeps the owning MemoryStore alive.  Callers hold MemoryStoreMatch
// values to prevent the MemoryStore (and its InMemorySeries) from being
// destroyed during background TSM conversion.
template <class T>
struct MemoryStoreMatch {
    seastar::shared_ptr<MemoryStore> store;
    const InMemorySeries<T>* series;
};

class WALFileManager {
private:
    int shardId;
    uint32_t currentWalSequenceNumber = 0;
    bool walSequenceInitialized_ = false;
    std::vector<seastar::shared_ptr<MemoryStore>> memoryStores;
    TSMFileManager* tsmFileManager;
    seastar::gate _backgroundGate;               // Tracks in-flight background TSM conversions
    seastar::semaphore compactionSemaphore{1};   // Only allow 1 rollover at a time
    seastar::semaphore _conversionSemaphore{1};  // Serialize background TSM conversions

public:
    WALFileManager();

    seastar::future<> init(Engine& engine, TSMFileManager& _tsmFileManager);
    template <class T>
    seastar::future<> insert(TimeStarInsert<T>& insertRequest);
    template <class T>
    seastar::future<> insertBatch(std::vector<TimeStarInsert<T>>& insertRequests);
    seastar::future<> rolloverMemoryStore();
    seastar::future<> convertWalToTsm(seastar::shared_ptr<MemoryStore> store);
    seastar::future<> close();
    std::optional<TSMValueType> getSeriesType(const std::string& seriesKey);
    std::optional<TSMValueType> getSeriesType(const SeriesId128& seriesId);

    // Query memory stores for data (deletion filtering removed - WAL replay handles current state)
    // Returns the first matching in-memory series together with a shared_ptr
    // that keeps the owning MemoryStore alive, or std::nullopt if not found.
    // NOTE: With background TSM conversion, multiple memory stores may contain
    // data for the same series. This returns only the first match. Use
    // queryAllMemoryStores() to get data from all stores.
    template <class T>
    std::optional<MemoryStoreMatch<T>> queryMemoryStores(const std::string& seriesKey) {
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
        return queryMemoryStores<T>(seriesId);
    }

    template <class T>
    std::optional<MemoryStoreMatch<T>> queryMemoryStores(const SeriesId128& seriesId) {
        for (auto& memStore : memoryStores) {
            auto result = memStore->querySeries<T>(seriesId);
            if (result != nullptr) {
                return MemoryStoreMatch<T>{memStore, result};
            }
        }
        return std::nullopt;
    }

    // Query ALL memory stores and return every matching series together with
    // a shared_ptr that keeps the owning MemoryStore alive.  The caller holds
    // the returned MemoryStoreMatch values to prevent use-after-free if a
    // background TSM conversion removes a store from the memoryStores vector.
    template <class T>
    std::vector<MemoryStoreMatch<T>> queryAllMemoryStores(const std::string& seriesKey) {
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
        return queryAllMemoryStores<T>(seriesId);
    }

    template <class T>
    std::vector<MemoryStoreMatch<T>> queryAllMemoryStores(const SeriesId128& seriesId) {
        return queryAllMemoryStores<T>(memoryStores, seriesId);
    }

    // Static overload operating on a caller-held (pinned) snapshot of the
    // memory-store list.  See pinMemoryStores() for the visibility contract.
    template <class T>
    static std::vector<MemoryStoreMatch<T>> queryAllMemoryStores(
        const std::vector<seastar::shared_ptr<MemoryStore>>& stores, const SeriesId128& seriesId) {
        std::vector<MemoryStoreMatch<T>> results;
        for (auto& memStore : stores) {
            auto result = memStore->querySeries<T>(seriesId);
            if (result != nullptr) {
                results.push_back(MemoryStoreMatch<T>{memStore, result});
            }
        }
        return results;
    }

    // Access the underlying memory stores (for batch operations that iterate
    // all stores once instead of per-series lookups).
    const std::vector<seastar::shared_ptr<MemoryStore>>& getMemoryStores() const { return memoryStores; }

    // Pin the current memory-store set for the duration of a query.
    //
    // VISIBILITY INVARIANT (WAL->TSM conversion): a background conversion
    // registers the new TSM file FIRST and only then erases the retiring
    // memory store from `memoryStores`.  A query therefore sees every point
    // at every instant iff it (1) pins the memory stores BEFORE snapshotting
    // the TSM file list and (2) reads only from the pinned copy afterwards.
    // The shared_ptr copies keep retiring stores (and their series data)
    // alive and queryable even if a conversion completes — and erases the
    // store from the live vector — while the query is suspended on TSM I/O.
    // Reading the LIVE vector after a co_await instead would open a window
    // where a just-converted series is visible in NEITHER source (the TSM
    // snapshot predates registration, the store is already gone).
    std::vector<seastar::shared_ptr<MemoryStore>> pinMemoryStores() const { return memoryStores; }

    // Return the earliest timestamp across all memory stores for a given series
    // within [startTime, endTime].  Returns nullopt if no memory data in range.
    // Used by pushdown aggregation to determine the split point between the
    // TSM-only portion (full pushdown) and the overlap portion (fallback).
    // Type-agnostic: one hash lookup per store (visits the series variant)
    // instead of one probe per candidate value type.  String series are
    // excluded to match the semantics of probing only aggregatable types.
    std::optional<uint64_t> getEarliestMemoryTimestampAnyType(const SeriesId128& seriesId, uint64_t startTime,
                                                              uint64_t endTime) {
        return getEarliestMemoryTimestampAnyType(memoryStores, seriesId, startTime, endTime);
    }

    // Static overload operating on a pinned snapshot (see pinMemoryStores()).
    static std::optional<uint64_t> getEarliestMemoryTimestampAnyType(
        const std::vector<seastar::shared_ptr<MemoryStore>>& stores, const SeriesId128& seriesId, uint64_t startTime,
        uint64_t endTime) {
        std::optional<uint64_t> earliest;
        for (auto& memStore : stores) {
            auto ts = memStore->earliestTimestampInRange(seriesId, startTime, endTime);
            if (ts && (!earliest.has_value() || *ts < *earliest)) {
                earliest = ts;
            }
        }
        return earliest;
    }

    // Delete data from memory stores and write to WAL
    seastar::future<> deleteFromMemoryStores(const std::string& seriesKey, uint64_t startTime, uint64_t endTime);
};
