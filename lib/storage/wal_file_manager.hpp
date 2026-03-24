#pragma once

#include "memory_store.hpp"
#include "tsm_file_manager.hpp"

#include <atomic>
#include <cstdint>
#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>
#include <vector>

class Engine;

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
        std::vector<MemoryStoreMatch<T>> results;
        for (auto& memStore : memoryStores) {
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

    // Check if ANY memory store has data for this series within [startTime, endTime].
    // Returns on first match without allocating a vector.  Used by pushdown
    // aggregation to decide whether to fall back to the full query path.
    template <class T>
    bool hasMemoryDataInRange(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime) {
        for (auto& memStore : memoryStores) {
            auto* series = memStore->template querySeries<T>(seriesId);
            if (series && !series->timestamps.empty()) {
                auto it = std::lower_bound(series->timestamps.begin(), series->timestamps.end(), startTime);
                if (it != series->timestamps.end() && *it <= endTime) {
                    return true;
                }
            }
        }
        return false;
    }

    // Return the earliest timestamp across all memory stores for a given series
    // within [startTime, endTime].  Returns nullopt if no memory data in range.
    // Used by pushdown aggregation to determine the split point between the
    // TSM-only portion (full pushdown) and the overlap portion (fallback).
    template <class T>
    std::optional<uint64_t> getEarliestMemoryTimestamp(const SeriesId128& seriesId, uint64_t startTime,
                                                       uint64_t endTime) {
        std::optional<uint64_t> earliest;
        for (auto& memStore : memoryStores) {
            auto* series = memStore->template querySeries<T>(seriesId);
            if (series && !series->timestamps.empty()) {
                auto it = std::lower_bound(series->timestamps.begin(), series->timestamps.end(), startTime);
                if (it != series->timestamps.end() && *it <= endTime) {
                    uint64_t ts = *it;
                    if (!earliest.has_value() || ts < *earliest) {
                        earliest = ts;
                    }
                }
            }
        }
        return earliest;
    }

    // Delete data from memory stores and write to WAL
    seastar::future<> deleteFromMemoryStores(const std::string& seriesKey, uint64_t startTime, uint64_t endTime);
};
