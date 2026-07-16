#include "memory_store.hpp"

#include "logger.hpp"
#include "logging_config.hpp"
#include "simd_aggregator.hpp"
#include "util.hpp"
#include "value_type_dispatch.hpp"  // TIMESTAR_INSTANTIATE_FOR_VALUE_TYPES

#include <algorithm>
#include <chrono>
#include <numeric>
#include <ranges>
#include <stdexcept>

// ---------------------------------------------------------------------------
// InMemorySeriesStats::update — maintain running aggregation stats on insert.
//
// Phase 1: sum/min/max via SIMD (no data dependency between iterations)
// Phase 2: Welford mean/m2 via scalar (sequential dependency)
// Phase 3: first/latest timestamp tracking (branch-heavy, not vectorizable)
// ---------------------------------------------------------------------------
void InMemorySeriesStats::update(const double* values, const uint64_t* timestamps, size_t n) {
    if (n == 0)
        return;
    valid = true;

    // Phase 1: FUSED SIMD sum/min/max — one memory pass instead of three
    // separate kernel calls.
    double batchSum, batchMin, batchMax;
    timestar::simd::SimdAggregator::calculateSumMinMax(values, n, batchSum, batchMin, batchMax);

    // Kahan compensated accumulation of batch sum
    double y = batchSum - sumCompensation;
    double t = sum + y;
    sumCompensation = (t - sum) - y;
    sum = t;

    if (batchMin < min)
        min = batchMin;
    if (batchMax > max)
        max = batchMax;

    // Phase 2: batch mean/M2 via SIMD two-pass + Chan's parallel merge.
    // Replaces the per-value Welford loop, whose `mean += delta / count`
    // put a hardware divide on a loop-carried dependency chain (~20 cy/pt).
    // Two-pass batch variance is numerically as good as sequential Welford.
    {
        const double batchMean = batchSum / static_cast<double>(n);
        // calculateVariance returns population variance (M2/n)
        const double batchM2 =
            timestar::simd::SimdAggregator::calculateVariance(values, n, batchMean) * static_cast<double>(n);
        if (count == 0) {
            mean = batchMean;
            m2 = batchM2;
        } else {
            const double delta = batchMean - mean;
            const double totalCount = static_cast<double>(count) + static_cast<double>(n);
            m2 += batchM2 + delta * delta * (static_cast<double>(count) * static_cast<double>(n)) / totalCount;
            mean += delta * (static_cast<double>(n) / totalCount);
        }
        count += n;
    }

    // Phase 3: first/latest timestamp tracking.
    // Batches are almost always sorted (chronological ingest), so the
    // endpoints suffice; the sortedness scan is a single predictable
    // compare per element vs. the two-branch tracking loop.
    if (std::is_sorted(timestamps, timestamps + n)) [[likely]] {
        if (timestamps[0] < firstTimestamp) {
            firstTimestamp = timestamps[0];
            firstValue = values[0];
        }
        if (timestamps[n - 1] >= latestTimestamp) {
            latestTimestamp = timestamps[n - 1];
            latestValue = values[n - 1];
        }
    } else {
        for (size_t i = 0; i < n; ++i) {
            uint64_t ts = timestamps[i];
            if (ts < firstTimestamp) {
                firstTimestamp = ts;
                firstValue = values[i];
            }
            if (ts >= latestTimestamp) {
                latestTimestamp = ts;
                latestValue = values[i];
            }
        }
    }
}

template <class T>
void InMemorySeries<T>::insert(TimeStarInsert<T>&& insertRequest) {
    const auto& srcTimestamps = insertRequest.getTimestamps();
    if (srcTimestamps.empty()) [[unlikely]]
        return;

    // Update running stats for double series (O(n) single pass over new data only)
    if constexpr (std::is_same_v<T, double>) {
        stats.update(insertRequest.values.data(), srcTimestamps.data(), srcTimestamps.size());
    }

    const size_t oldSize = timestamps.size();
    const size_t newSize = srcTimestamps.size();

    if (oldSize == 0) [[unlikely]] {
        // Series is empty -- take ownership of the entire vectors via move.
        // takeTimestamps() materializes shared timestamps into owned if needed.
        timestamps = insertRequest.takeTimestamps();
        values = std::move(insertRequest.values);

        // First insert -- check if already sorted using std::ranges::is_sorted.
        // In typical time-series workloads, data arrives chronologically, so this
        // check short-circuits the sort in the common case.
        if (!std::ranges::is_sorted(timestamps)) [[unlikely]] {
            sortPaired(0, timestamps.size());
        }
        return;
    }

    if (newSize > SIZE_MAX - oldSize) {
        throw std::runtime_error("InMemorySeries: size overflow");
    }
    const size_t totalSize = oldSize + newSize;
    // Grow geometrically when more capacity is needed. An exact-size reserve
    // here would leave capacity == size after every append, forcing the NEXT
    // append to realloc and memcpy the entire series — O(L) copied bytes per
    // batch instead of amortized O(1).
    if (timestamps.capacity() < totalSize) {
        const size_t growCap = std::max(totalSize, oldSize * 2);
        timestamps.reserve(growCap);
        values.reserve(growCap);
    }

    // Append new data. insert() with iterators is optimized by the STL to
    // perform a single memcpy/memmove for trivially copyable types (uint64_t, double, bool).
    timestamps.insert(timestamps.end(), srcTimestamps.begin(), srcTimestamps.end());
    values.insert(values.end(), std::make_move_iterator(insertRequest.values.begin()),
                  std::make_move_iterator(insertRequest.values.end()));

    // Fast path: check if the combined data is already sorted.
    // In typical time-series workloads, new data arrives in chronological order
    // and the first new timestamp >= last old timestamp, making this the common case.
    const bool boundaryOk = timestamps[oldSize] >= timestamps[oldSize - 1];
    if (boundaryOk) [[likely]] {
        // Boundary is fine -- check if new data itself is sorted.
        if (std::ranges::is_sorted(std::ranges::subrange(timestamps.begin() + oldSize, timestamps.end()))) [[likely]] {
            return;  // Everything already sorted, no work needed
        }

        // New suffix is unsorted but boundary was ok. Sort just the suffix, then
        // re-check the boundary. If the minimum of the suffix (after sorting) is
        // still >= old tail, we avoid a full merge.
        sortPaired(oldSize, totalSize);

        if (timestamps[oldSize] >= timestamps[oldSize - 1]) [[likely]] {
            return;  // Suffix sorted and boundary still clean
        }
        // Fall through to merge: suffix is now sorted but boundary is violated
        // (sorting moved a smaller timestamp to the front of the suffix).
    } else {
        // Boundary is violated. Sort the suffix first so we can merge two sorted runs.
        if (!std::ranges::is_sorted(std::ranges::subrange(timestamps.begin() + oldSize, timestamps.end()))) {
            sortPaired(oldSize, totalSize);
        }
    }

    // Both [0, oldSize) and [oldSize, totalSize) are now individually sorted.
    // Use inplace_merge on paired data to combine them in O(n) time
    // (vs O(n log n) for a full sort).
    mergePaired(oldSize);
}

template <class T>
void InMemorySeries<T>::sortPaired(size_t from, size_t to) {
    const size_t count = to - from;
    if (count <= 1) [[unlikely]]
        return;

    // Build index array for the range [from, to). Sorting indices lets us apply
    // the same permutation to both timestamps and values in a single pass.
    std::vector<size_t> indices(count);
    std::iota(indices.begin(), indices.end(), from);

    std::sort(indices.begin(), indices.end(), [this](size_t a, size_t b) { return timestamps[a] < timestamps[b]; });

    // Apply permutation via temporary timestamp vector. Values are applied
    // differently based on type: std::vector<bool> uses proxy references
    // that don't support std::move, so we use copy for that specialization.
    std::vector<uint64_t> sortedTs(count);
    for (size_t i = 0; i < count; ++i) {
        sortedTs[i] = timestamps[indices[i]];
    }
    std::copy(sortedTs.begin(), sortedTs.end(), timestamps.begin() + from);

    if constexpr (std::is_same_v<T, bool>) {
        std::vector<bool> sortedVals(count);
        for (size_t i = 0; i < count; ++i) {
            sortedVals[i] = static_cast<bool>(values[indices[i]]);
        }
        std::copy(sortedVals.begin(), sortedVals.end(), values.begin() + from);
    } else {
        std::vector<T> sortedVals(count);
        for (size_t i = 0; i < count; ++i) {
            sortedVals[i] = std::move(values[indices[i]]);
        }
        std::move(sortedVals.begin(), sortedVals.end(), values.begin() + from);
    }
}

template <class T>
void InMemorySeries<T>::mergePaired(size_t midpoint) {
    const size_t n = timestamps.size();
    if (midpoint == 0 || midpoint >= n) [[unlikely]]
        return;

    // Merge two sorted runs: [0, midpoint) and [midpoint, n).
    // We use a single-pass merge into temporary buffers. This is O(n) time
    // and O(n) space, but avoids the index indirection overhead of the old
    // approach and has better cache locality (sequential reads from both runs,
    // sequential writes to output).
    std::vector<uint64_t> mergedTs;
    std::vector<T> mergedVals;
    mergedTs.reserve(n);
    mergedVals.reserve(n);

    size_t i = 0, j = midpoint;
    while (i < midpoint && j < n) {
        if (timestamps[i] <= timestamps[j]) {
            mergedTs.push_back(timestamps[i]);
            mergedVals.push_back(std::move(values[i]));
            ++i;
        } else {
            mergedTs.push_back(timestamps[j]);
            mergedVals.push_back(std::move(values[j]));
            ++j;
        }
    }
    // Append remaining elements from whichever run is not exhausted
    while (i < midpoint) {
        mergedTs.push_back(timestamps[i]);
        mergedVals.push_back(std::move(values[i]));
        ++i;
    }
    while (j < n) {
        mergedTs.push_back(timestamps[j]);
        mergedVals.push_back(std::move(values[j]));
        ++j;
    }

    timestamps = std::move(mergedTs);
    values = std::move(mergedVals);
}

seastar::future<> MemoryStore::close() {
    if (closed)
        co_return;
    closed = true;

    if (wal) {
        timestar::memory_log.debug("Closing WAL for memory store {}", sequenceNumber);
        co_await wal->close();
    }
}

seastar::future<> MemoryStore::initWAL() {
    wal = std::make_unique<WAL>(sequenceNumber);
    co_await wal->init(this, false);  // false = not recovery, create fresh WAL
}

seastar::future<> MemoryStore::removeWAL() {
    if (wal) {
        co_await wal->remove();
        wal.reset();
    }
}

seastar::future<> MemoryStore::initFromWAL(std::string filename) {
    WALReader reader(filename);
    co_await reader.readAll(this);
    // Don't initialize a new WAL here - this memory store is being recovered
    // from an existing WAL that will be converted to TSM and removed
}

bool MemoryStore::isFull() const {
    if (!wal) {
        return false;
    }

    // Use the larger of actual WAL size and cumulative estimated size.
    // Compression can make the actual WAL size much smaller than estimates,
    // so relying only on actual size may never trigger rollover.
    size_t walSize = wal->getCurrentSize();
    size_t effectiveSize = std::max(walSize, estimatedAccumulatedSize);
    return effectiveSize >= walSizeThreshold();
}

template <class T>
void MemoryStore::insertMemory(TimeStarInsert<T>&& insertRequest) {
    // In-memory insert
    SeriesId128 seriesId = insertRequest.seriesId128();

    // robin_map's operator[] returns a mutable reference, creating entry if needed.
    // Default-constructed variant will be InMemorySeries<double> (first alternative).
    auto& variantSeries = series[seriesId];

    // Single variant access: std::get_if returns a pointer if the type matches, nullptr otherwise.
    // This replaces the previous 2-3 separate variant accesses
    // (std::visit for empty check + std::holds_alternative for type check + std::get for access).
    auto* typedSeries = std::get_if<InMemorySeries<T>>(&variantSeries);

    if (!typedSeries || typedSeries->timestamps.empty()) {
        // Either wrong type (e.g. default-constructed double when T != double) or empty.
        // Before overwriting, check whether the current variant has existing data of a different type.
        if (typedSeries == nullptr) {
            bool hasData = std::visit([](const auto& s) { return !s.timestamps.empty(); }, variantSeries);
            if (hasData) {
                throw std::runtime_error("Type mismatch: series " + insertRequest.seriesKey() +
                                         " already exists with a different type");
            }
        }
        variantSeries = InMemorySeries<T>();
        typedSeries = &std::get<InMemorySeries<T>>(variantSeries);
    }

    typedSeries->insert(std::move(insertRequest));
}

template <class T>
bool MemoryStore::wouldExceedThreshold(TimeStarInsert<T>& insertRequest) {
    size_t sz = 0;
    return wouldExceedThreshold(insertRequest, sz);
}

template <class T>
bool MemoryStore::wouldExceedThreshold(TimeStarInsert<T>& insertRequest, size_t& outEstimatedSize) {
    if (!wal) {
        outEstimatedSize = 0;
        return false;
    }

    outEstimatedSize = wal->estimateInsertSize(insertRequest);
    // Use the larger of actual WAL size and cumulative estimated size for
    // the base, ensuring rollover triggers even when compression is highly
    // effective and actual WAL size grows slowly.
    size_t effectiveSize = std::max(wal->getCurrentSize(), estimatedAccumulatedSize);

    return (effectiveSize + outEstimatedSize) >= walSizeThreshold();
}

template <class T>
seastar::future<bool> MemoryStore::insert(TimeStarInsert<T>& insertRequest) {
    if (closed)
        throw std::runtime_error("MemoryStore is closed");

    // Check if this insert would exceed the 16MB WAL threshold.
    // The estimated size is returned via outEstimatedSize to avoid
    // recomputing it below (eliminates double-estimation).
    size_t thisEstimatedSize = 0;
    bool needsRollover = wouldExceedThreshold(insertRequest, thisEstimatedSize);
    if (needsRollover) {
        // Don't insert - signal that rollover is needed
        timestar::memory_log.debug("Insert would exceed 16MB WAL limit, signaling rollover needed");
        co_return true;
    }

    if (wal) {
        auto walResult = co_await wal->insert(insertRequest);
        if (walResult == WALInsertResult::RolloverNeeded) [[unlikely]] {
            co_return true;
        }
        LOG_INSERT_PATH(timestar::memory_log, trace, "WAL insert completed for series: {}", insertRequest.seriesKey());
    }

    estimatedAccumulatedSize += thisEstimatedSize;
    insertMemory(std::move(insertRequest));

    co_return false;
}

template <class T>
seastar::future<bool> MemoryStore::insertBatch(std::vector<TimeStarInsert<T>>& insertRequests,
                                               size_t preComputedBatchSize) {
    if (closed)
        throw std::runtime_error("MemoryStore is closed");

    if (insertRequests.empty()) {
        co_return false;  // No work to do
    }

#if TIMESTAR_LOG_INSERT_PATH
    auto start_memory_batch = std::chrono::high_resolution_clock::now();
#endif
    LOG_INSERT_PATH(timestar::memory_log, info, "[PERF] [MEMORY] Batch insert started for {} requests",
                    insertRequests.size());

    // Compute the estimated batch size if not pre-computed
    size_t batchEstimate = preComputedBatchSize;
    if (batchEstimate == 0 && wal) {
        for (auto& insertRequest : insertRequests) {
            batchEstimate += wal->estimateInsertSize(insertRequest);
        }
    }

    // Check if this batch would exceed the WAL threshold.
    // Use the larger of actual WAL size and cumulative estimated size as the
    // base. Compression can make the actual WAL size much smaller than the
    // uncompressed estimate, so relying only on actual compressed size may
    // never trigger rollover for highly compressible data.
    bool needsRollover = false;
    if (wal) {
        size_t effectiveSize = std::max(wal->getCurrentSize(), estimatedAccumulatedSize);
        needsRollover = (effectiveSize + batchEstimate) >= walSizeThreshold();
    }
    if (needsRollover) {
        // Don't insert - signal that rollover is needed
        LOG_INSERT_PATH(timestar::memory_log, debug, "Batch insert would exceed WAL limit, signaling rollover needed");
        co_return true;
    }

#if TIMESTAR_LOG_INSERT_PATH
    auto start_wal_insert = std::chrono::high_resolution_clock::now();
#endif
    if (wal) {
        auto walResult = co_await wal->insertBatch(insertRequests);
        if (walResult == WALInsertResult::RolloverNeeded) [[unlikely]] {
            co_return true;
        }
    }

    // Increment AFTER the WAL insert succeeds — if the WAL throws, we must not
    // leak the estimate (there is no corresponding decrement on the exception path).
    estimatedAccumulatedSize += batchEstimate;

#if TIMESTAR_LOG_INSERT_PATH
    auto start_memory_insert = std::chrono::high_resolution_clock::now();
#endif
    for (auto& insertRequest : insertRequests) {
        // Bind the cached key by const-ref: insertMemory() moves out only values and
        // timestamps, never _seriesKey, and insertRequest stays a live lvalue, so the
        // reference is valid in the catch block (the only reader). Avoids a per-point
        // string copy on the happy path.
        const std::string& key = insertRequest.seriesKey();
        try {
            insertMemory(std::move(insertRequest));
        } catch (const std::exception& e) {
            timestar::memory_log.warn(
                "insertMemory failed for series {}: {} — data is in WAL and will be recovered on restart", key,
                e.what());
        }
    }
#if TIMESTAR_LOG_INSERT_PATH
    auto end_memory_batch = std::chrono::high_resolution_clock::now();
    auto wal_duration = std::chrono::duration_cast<std::chrono::microseconds>(start_memory_insert - start_wal_insert);
    auto mem_duration = std::chrono::duration_cast<std::chrono::microseconds>(end_memory_batch - start_memory_insert);
    LOG_INSERT_PATH(timestar::memory_log, info, "[PERF] [MEMORY] WAL batch: {}μs, memory: {}μs", wal_duration.count(),
                    mem_duration.count());
#endif

    co_return false;
}

std::optional<TSMValueType> MemoryStore::getSeriesType(const SeriesId128& seriesId) const {
    // Compile-time guard: variant order must match TSMValueType enum values.
    using V = decltype(series)::mapped_type;
    static_assert(std::is_same_v<std::variant_alternative_t<0, V>, InMemorySeries<double>>);
    static_assert(std::is_same_v<std::variant_alternative_t<1, V>, InMemorySeries<bool>>);
    static_assert(std::is_same_v<std::variant_alternative_t<2, V>, InMemorySeries<std::string>>);
    static_assert(std::is_same_v<std::variant_alternative_t<3, V>, InMemorySeries<int64_t>>);

    auto it = series.find(seriesId);

    if (it == series.end())
        return {};

    return static_cast<TSMValueType>(it->second.index());
}

// Explicit template instantiations for every TimeStar value type.
// The macro fans out (double, bool, std::string, int64_t) — see
// value_type_dispatch.hpp.
#define INST_INMEM(T)                                              \
    template void InMemorySeries<T>::insert(TimeStarInsert<T>&&);  \
    template void InMemorySeries<T>::sortPaired(size_t, size_t);   \
    template void InMemorySeries<T>::mergePaired(size_t);
TIMESTAR_INSTANTIATE_FOR_VALUE_TYPES(INST_INMEM)
#undef INST_INMEM

#define INST_STORE(T)                                                                                      \
    template seastar::future<bool> MemoryStore::insert<T>(TimeStarInsert<T>&);                             \
    template seastar::future<bool> MemoryStore::insertBatch<T>(std::vector<TimeStarInsert<T>>&, size_t);   \
    template void MemoryStore::insertMemory<T>(TimeStarInsert<T>&&);                                       \
    template bool MemoryStore::wouldExceedThreshold<T>(TimeStarInsert<T>&);                                \
    template bool MemoryStore::wouldExceedThreshold<T>(TimeStarInsert<T>&, size_t&);
TIMESTAR_INSTANTIATE_FOR_VALUE_TYPES(INST_STORE)
#undef INST_STORE

void MemoryStore::deleteRange(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime) {
    timestar::memory_log.debug("Deleting range for series {} from {} to {}", seriesId.toHex(), startTime, endTime);

    // Find the series in memory
    auto it = series.find(seriesId);
    if (it == series.end()) {
        return;  // No data to delete
    }

    auto& variantSeries = it.value();

    // Actually remove data from memory based on variant type
    std::visit(
        [&](auto& inMemorySeries) {
            auto& timestamps = inMemorySeries.timestamps;
            auto& values = inMemorySeries.values;

            // Binary search for the deletion range on sorted timestamps
            auto startIt = std::lower_bound(timestamps.begin(), timestamps.end(), startTime);
            auto endIt = std::upper_bound(timestamps.begin(), timestamps.end(), endTime);

            if (startIt == endIt) {
                return;  // Nothing to delete
            }

            size_t startIdx = static_cast<size_t>(startIt - timestamps.begin());
            size_t endIdx = static_cast<size_t>(endIt - timestamps.begin());

            timestamps.erase(timestamps.begin() + startIdx, timestamps.begin() + endIdx);
            values.erase(values.begin() + startIdx, values.begin() + endIdx);

            // Invalidate running stats after deletion — they may no longer
            // reflect the actual data (min/max could have been in deleted range).
            inMemorySeries.stats.valid = false;
        },
        variantSeries);

    // If all data was removed, remove the series entirely
    bool isEmpty =
        std::visit([](const auto& inMemorySeries) { return inMemorySeries.timestamps.empty(); }, variantSeries);

    if (isEmpty) {
        series.erase(seriesId);
    }
}