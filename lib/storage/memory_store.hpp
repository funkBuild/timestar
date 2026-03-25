#pragma once

#include "logger.hpp"
#include "series_id.hpp"
#include "timestar_config.hpp"
#include "timestar_value.hpp"
#include "tsm.hpp"
#include "wal.hpp"

#include <tsl/robin_map.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <variant>
#include <vector>

class WAL;

// Running statistics for a float series in memory — maintained during insert.
// Enables block-stats-like pushdown aggregation without per-point scanning
// when the query range covers the entire series.
//
// Split into two phases:
//   1. sum/min/max via SIMD (no data dependency, vectorizable)
//   2. Welford mean/m2 via scalar (sequential dependency on mean)
struct InMemorySeriesStats {
    double sum = 0.0;
    double sumCompensation = 0.0;  // Kahan compensated sum
    double min = std::numeric_limits<double>::max();
    double max = std::numeric_limits<double>::lowest();
    uint64_t count = 0;
    // Welford's online variance accumulators
    double mean = 0.0;
    double m2 = 0.0;
    // First/Latest tracking
    double firstValue = 0.0;
    uint64_t firstTimestamp = std::numeric_limits<uint64_t>::max();
    double latestValue = 0.0;
    uint64_t latestTimestamp = 0;

    bool valid = false;  // false until first update

    // Compensated sum accessor matching AggregationState convention
    double compensatedSum() const { return sum + sumCompensation; }

    void update(const double* values, const uint64_t* timestamps, size_t n);
};

template <class T>
class InMemorySeries {
public:
    std::vector<uint64_t> timestamps;
    std::vector<T> values;

    // Running stats — only valid for double series, updated on each insert.
    // Used by pushdown aggregation to skip per-point scanning when query
    // covers the entire series time range.
    InMemorySeriesStats stats;

    void insert(TimeStarInsert<T>&& insertRequest);

    // Sort all timestamps and values together. Convenience wrapper for sortPaired()
    // over the entire range, used by TSM writer before flushing to disk.
    void sort() { sortPaired(0, timestamps.size()); }

    // Sort a subrange [from, to) of timestamps and values together using index-based
    // permutation. Only allocates temporary storage proportional to the subrange size,
    // not the full vector. Used for sorting unsorted suffixes before merging.
    void sortPaired(size_t from, size_t to);

    // Merge two individually-sorted runs [0, midpoint) and [midpoint, n) into a
    // single sorted sequence. Uses a linear-time two-pointer merge into temporary
    // buffers, which is O(n) vs O(n log n) for a full re-sort.
    void mergePaired(size_t midpoint);
};

// Order of variants must match TSMValueType order
using VariantInMemorySeries =
    std::variant<InMemorySeries<double>, InMemorySeries<bool>, InMemorySeries<std::string>, InMemorySeries<int64_t>>;

class MemoryStore {
private:
    std::unique_ptr<WAL> wal;
    bool closed = false;
    // Tracks cumulative estimated (worst-case/pre-compression) insert sizes.
    // The actual compressed WAL size grows much slower than estimates due to
    // XOR float compression and FFOR timestamp compression.  Using only
    // the compressed size for rollover decisions can cause the WAL to never
    // trigger rollover when compression is highly effective.  This counter
    // provides a conservative upper bound for rollover decisions.
    size_t estimatedAccumulatedSize = 0;

public:
    // Threshold for WAL rollover decisions (based on estimated sizes).
    // Read from timestar::config().storage.wal_size_threshold at runtime.
    static size_t walSizeThreshold() { return timestar::config().storage.wal_size_threshold; }
    const unsigned int sequenceNumber;
    // Use robin_map for O(1) lookups with better cache locality than std::unordered_map
    tsl::robin_map<SeriesId128, VariantInMemorySeries, SeriesId128::Hash> series;

    MemoryStore(unsigned int _sequenceNumber) : sequenceNumber(_sequenceNumber) {
        timestar::memory_log.debug("Memory store {} created", sequenceNumber);
    }
    ~MemoryStore() { timestar::memory_log.debug("Memory store {} removed", sequenceNumber); }

    seastar::future<> initWAL();
    seastar::future<> removeWAL();
    seastar::future<> initFromWAL(std::string filename);
    seastar::future<> close();
    template <class T>
    void insertMemory(TimeStarInsert<T>&& insertRequest);
    // Insert a single series write. Returns true if WAL needs rollover.
    // On success (returns false), insertRequest is moved-from and must not be reused.
    // On rollover (returns true), insertRequest is NOT consumed and can be retried.
    template <class T>
    seastar::future<bool> insert(TimeStarInsert<T>& insertRequest);

    template <class T>
    seastar::future<bool> insertBatch(
        std::vector<TimeStarInsert<T>>& insertRequests,
        size_t preComputedBatchSize = 0);  // Batch insert - returns true if WAL needs rollover
    bool isFull() const;
    // Takes non-const ref because WAL::estimateInsertSize() caches the computed
    // size in insertRequest._cachedEstimatedSize (mutable field).
    template <class T>
    bool wouldExceedThreshold(TimeStarInsert<T>& insertRequest);
    // Variant that also returns the computed estimated size to avoid
    // double-estimation in the single-insert path.
    template <class T>
    bool wouldExceedThreshold(TimeStarInsert<T>& insertRequest, size_t& outEstimatedSize);
    template <class T>
    bool wouldBatchExceedThreshold(std::vector<TimeStarInsert<T>>& insertRequests);
    bool isClosed() const { return closed; }
    bool isEmpty() const { return series.empty(); }
    std::optional<TSMValueType> getSeriesType(const SeriesId128& seriesId) const;
    WAL* getWAL() { return wal.get(); }

    // Query method to get data for a series.
    // Returns a const pointer to avoid copying the entire series data.
    // Returns nullptr if the series is not found or has a different type.
    template <class T>
    const InMemorySeries<T>* querySeries(const SeriesId128& seriesId) const {
        auto it = series.find(seriesId);
        if (it != series.end()) {
            return std::visit(
                [](const auto& arg) -> const InMemorySeries<T>* {
                    using SeriesType = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<SeriesType, InMemorySeries<T>>) {
                        return &arg;
                    }
                    return nullptr;
                },
                it->second);
        }
        return nullptr;
    }

    // Delete data in a time range for a series
    void deleteRange(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime);
};
