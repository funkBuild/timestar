#include "memory_store.hpp"

#include "logger.hpp"
#include "logging_config.hpp"
#include "simd_aggregator.hpp"
#include "util.hpp"
#include "value_type_dispatch.hpp"  // TIMESTAR_INSTANTIATE_FOR_VALUE_TYPES

#include <algorithm>
#include <chrono>
#include <cmath>
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
    // separate kernel calls. The fused kernel NaN-skips min/max but the sum
    // propagates NaN, and its all-skipped sentinel collapses ±Inf results to
    // NaN — both cases route to the scalar special-value pass below.
    double batchSum, batchMin, batchMax;
    timestar::simd::SimdAggregator::calculateSumMinMax(values, n, batchSum, batchMin, batchMax);

    // Number of non-NaN values in this batch (canonical: NaN = missing data,
    // never counted/summed — docs/nan_policy.md).
    size_t validN = n;

    if (std::isnan(batchSum) || std::isnan(batchMin) || std::isnan(batchMax)) [[unlikely]] {
        // Special values present: NaN data (skip it), an Inf/-Inf mix (sum is
        // genuinely NaN), or all-±Inf data (kernel sentinel conflation).
        // Recompute scalar with NaN skipped; ±Inf participates arithmetically.
        batchSum = 0.0;
        batchMin = std::numeric_limits<double>::infinity();
        batchMax = -std::numeric_limits<double>::infinity();
        validN = 0;
        for (size_t i = 0; i < n; ++i) {
            const double v = values[i];
            if (std::isnan(v))
                continue;
            ++validN;
            batchSum += v;
            if (v < batchMin)
                batchMin = v;
            if (v > batchMax)
                batchMax = v;
        }
        if (validN == 0)
            return;  // All-NaN batch: nothing to fold (count unchanged).
    }

    // Kahan compensated accumulation of batch sum.
    // Non-finite guard: with a ±Inf sum the compensation term degenerates to
    // NaN and would poison later batches (Inf results silently become NaN).
    double y = batchSum - sumCompensation;
    double t = sum + y;
    sumCompensation = std::isfinite(t) ? (t - sum) - y : 0.0;
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
        const double batchMean = batchSum / static_cast<double>(validN);
        // calculateVariance returns population variance (M2/n).
        // With NaN present the SIMD kernel would return NaN — compute the
        // NaN-skipping variance scalar instead (rare path).
        double batchM2;
        if (validN == n) [[likely]] {
            batchM2 = timestar::simd::SimdAggregator::calculateVariance(values, n, batchMean) * static_cast<double>(n);
        } else {
            batchM2 = 0.0;
            for (size_t i = 0; i < n; ++i) {
                if (std::isnan(values[i]))
                    continue;
                const double delta = values[i] - batchMean;
                batchM2 += delta * delta;
            }
        }
        if (count == 0) {
            mean = batchMean;
            m2 = batchM2;
        } else {
            const double delta = batchMean - mean;
            const double totalCount = static_cast<double>(count) + static_cast<double>(validN);
            m2 += batchM2 + delta * delta * (static_cast<double>(count) * static_cast<double>(validN)) / totalCount;
            mean += delta * (static_cast<double>(validN) / totalCount);
        }
        count += validN;
    }

    // Phase 3: first/latest timestamp tracking.
    // Batches are almost always sorted (chronological ingest), so the
    // endpoints suffice; the sortedness scan is a single predictable
    // compare per element vs. the two-branch tracking loop.
    // When the batch contains NaN (validN < n), first/latest must be the
    // first/last non-NaN VALUES (canonical FIRST/LATEST semantics) — take
    // the NaN-skipping loop instead.
    if (validN == n && std::is_sorted(timestamps, timestamps + n)) [[likely]] {
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
            if (std::isnan(values[i]))
                continue;
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

    const size_t oldSize = timestamps.size();
    const size_t newSize = srcTimestamps.size();

    // Last-write-wins invariant: this method never leaves two points with the
    // same timestamp in the series. Duplicates within a batch keep the last
    // occurrence (request order); duplicates against existing data keep the
    // incoming value. Running stats are updated with the surviving new points
    // only, and fully recomputed when an existing point was overwritten.

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
        dedupSuffixKeepLast(0);

        if constexpr (std::is_same_v<T, double>) {
            stats.update(values.data(), timestamps.data(), timestamps.size());
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

    // Sort the suffix if needed. In typical time-series workloads new data
    // arrives in chronological order, so the is_sorted scan short-circuits
    // the (stable) sort in the common case.
    if (!std::ranges::is_sorted(std::ranges::subrange(timestamps.begin() + oldSize, timestamps.end()))) [[unlikely]] {
        sortPaired(oldSize, totalSize);
    }
    // Collapse duplicates within the batch: last write in request order wins.
    dedupSuffixKeepLast(oldSize);

    // Update running stats with the surviving new points. If the merge below
    // overwrites existing points, stats are recomputed from scratch instead.
    if constexpr (std::is_same_v<T, double>) {
        stats.update(values.data() + oldSize, timestamps.data() + oldSize, timestamps.size() - oldSize);
    }

    // Fast path: strictly appending (first new timestamp is later than the
    // old tail). Equal timestamps must NOT take this path — that's an
    // overwrite of the old tail point, handled by the merge.
    if (timestamps[oldSize] > timestamps[oldSize - 1]) [[likely]] {
        return;
    }

    // Both [0, oldSize) and [oldSize, end) are sorted and internally
    // duplicate-free. Merge in O(n); on timestamp collisions the suffix
    // (newer write) wins.
    const bool overwrote = mergePaired(oldSize);
    if (overwrote) {
        if constexpr (std::is_same_v<T, double>) {
            recomputeStats();
        }
    }
}

template <class T>
bool InMemorySeries<T>::dedupSuffixKeepLast(size_t from) {
    const size_t n = timestamps.size();
    if (n - from < 2)
        return false;
    // Fast pre-check: no adjacent equal timestamps means nothing to collapse
    // (the suffix is sorted, so duplicates would be adjacent).
    if (std::adjacent_find(timestamps.begin() + from, timestamps.end()) == timestamps.end()) [[likely]] {
        return false;
    }

    // Two-pointer compaction keeping the LAST value of each equal-timestamp
    // run. Stable sort preserved request order, so "last" == newest write.
    size_t w = from;
    for (size_t r = from + 1; r < n; ++r) {
        if (timestamps[r] == timestamps[w]) {
            values[w] = std::move(values[r]);
        } else {
            ++w;
            if (w != r) {
                timestamps[w] = timestamps[r];
                values[w] = std::move(values[r]);
            }
        }
    }
    ++w;
    timestamps.resize(w);
    values.resize(w);
    return true;
}

template <class T>
void InMemorySeries<T>::recomputeStats() {
    if constexpr (std::is_same_v<T, double>) {
        stats = InMemorySeriesStats{};
        stats.update(values.data(), timestamps.data(), timestamps.size());
    }
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

    // Stable: equal timestamps keep request order so last-write-wins dedup
    // can identify the newest write as the last element of each run.
    std::stable_sort(indices.begin(), indices.end(),
                     [this](size_t a, size_t b) { return timestamps[a] < timestamps[b]; });

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
bool InMemorySeries<T>::mergePaired(size_t midpoint) {
    const size_t n = timestamps.size();
    if (midpoint == 0 || midpoint >= n) [[unlikely]]
        return false;

    // Merge two sorted runs: [0, midpoint) and [midpoint, n).
    // We use a single-pass merge into temporary buffers. This is O(n) time
    // and O(n) space, but avoids the index indirection overhead of the old
    // approach and has better cache locality (sequential reads from both runs,
    // sequential writes to output).
    // Both runs are internally duplicate-free; on equal timestamps the suffix
    // run (newer write) wins and the prefix point is dropped (LWW).
    std::vector<uint64_t> mergedTs;
    std::vector<T> mergedVals;
    mergedTs.reserve(n);
    mergedVals.reserve(n);

    bool overwrote = false;
    size_t i = 0, j = midpoint;
    while (i < midpoint && j < n) {
        if (timestamps[i] < timestamps[j]) {
            mergedTs.push_back(timestamps[i]);
            mergedVals.push_back(std::move(values[i]));
            ++i;
        } else if (timestamps[j] < timestamps[i]) {
            mergedTs.push_back(timestamps[j]);
            mergedVals.push_back(std::move(values[j]));
            ++j;
        } else {
            // Equal timestamp: keep the incoming value, drop the old point.
            mergedTs.push_back(timestamps[j]);
            mergedVals.push_back(std::move(values[j]));
            ++i;
            ++j;
            overwrote = true;
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
    return overwrote;
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
    // Resident bytes are checked independently of the WAL: the WAL estimates
    // below are all about on-disk size; neither sees per-series overhead or
    // uncompressed string payloads, so a high-cardinality or string-heavy
    // store can hold hundreds of MB while they still read as near-zero.
    if (residentBytesEstimate >= residentBytesThreshold()) {
        return true;
    }

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

    // Account resident cost BEFORE inserting. Point cost is the real in-memory
    // width (timestamp + value, plus the payload for strings), not the compressed
    // WAL estimate; series cost is charged once, when the series first appears.
    const size_t pointCount = insertRequest.getTimestamps().size();
    size_t pointBytes = pointCount * (sizeof(uint64_t) + sizeof(T));
    if constexpr (std::is_same_v<T, std::string>) {
        for (const auto& v : insertRequest.values) {
            pointBytes += v.capacity();
        }
    }
    // robin_map's operator[] returns a mutable reference, creating entry if needed.
    // Default-constructed variant will be InMemorySeries<double> (first alternative).
    // Newness is derived from the size delta — no separate find() probe on the
    // hottest path in the system.
    const size_t sizeBefore = series.size();
    auto& variantSeries = series[seriesId];
    const bool isNewSeries = (series.size() != sizeBefore);
    residentBytesEstimate += pointBytes + (isNewSeries ? PER_SERIES_OVERHEAD_BYTES : 0);

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
    outEstimatedSize = wal ? wal->estimateInsertSize(insertRequest) : 0;

    // Roll over on RESIDENT bytes first — deliberately not gated on the WAL.
    // The WAL estimates below model compressed on-disk size, which wildly
    // under-counts RAM for high-cardinality or string-heavy data (see
    // residentBytesEstimate). The projection uses only the fixed point
    // width — string payloads are charged as they land in insertMemory(),
    // so at worst rollover triggers one insert late.
    const size_t incomingResident = insertRequest.getTimestamps().size() * (sizeof(uint64_t) + sizeof(T));
    if ((residentBytesEstimate + incomingResident) >= residentBytesThreshold()) {
        return true;
    }

    if (!wal) {
        return false;
    }

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
    if (!needsRollover) {
        // Roll over on RESIDENT bytes too — not gated on the WAL. The WAL
        // estimate models compressed on-disk size and under-counts RAM badly
        // for high-cardinality or string-heavy batches (see
        // residentBytesEstimate). Fixed point width only; string payloads are
        // charged as they land in insertMemory().
        size_t batchPoints = 0;
        for (const auto& insertRequest : insertRequests) {
            batchPoints += insertRequest.getTimestamps().size();
        }
        const size_t batchResident = batchPoints * (sizeof(uint64_t) + sizeof(T));
        needsRollover = (residentBytesEstimate + batchResident) >= residentBytesThreshold();
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
#define INST_INMEM(T)                                             \
    template void InMemorySeries<T>::insert(TimeStarInsert<T>&&); \
    template void InMemorySeries<T>::sortPaired(size_t, size_t);  \
    template bool InMemorySeries<T>::mergePaired(size_t);         \
    template bool InMemorySeries<T>::dedupSuffixKeepLast(size_t); \
    template void InMemorySeries<T>::recomputeStats();
TIMESTAR_INSTANTIATE_FOR_VALUE_TYPES(INST_INMEM)
#undef INST_INMEM

#define INST_STORE(T)                                                                                    \
    template seastar::future<bool> MemoryStore::insert<T>(TimeStarInsert<T>&);                           \
    template seastar::future<bool> MemoryStore::insertBatch<T>(std::vector<TimeStarInsert<T>>&, size_t); \
    template void MemoryStore::insertMemory<T>(TimeStarInsert<T>&&);                                     \
    template bool MemoryStore::wouldExceedThreshold<T>(TimeStarInsert<T>&);                              \
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