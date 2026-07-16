#pragma once

#include "aggregator.hpp"
#include "simd_aggregator.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace timestar {

// Stateful sink that folds decoded (timestamp, value) pairs directly into
// aggregated data, avoiding intermediate vector materialisation.
//
// Bucketed mode (interval > 0): values are folded into time-bucket states,
// reducing N data points to M buckets.
// Non-bucketed mode (interval == 0): raw (timestamp, value) pairs are stored
// as compact parallel vectors (16 bytes/point instead of 96 bytes/point with
// AggregationState), deferring state construction to the merge phase.
class BlockAggregator {
public:
    // Maximum number of buckets to pre-allocate (guards against pathological inputs
    // such as a tiny interval over a very large time range).
    static constexpr size_t MAX_PREALLOCATED_BUCKETS = 100'000;

    // Maximum number of per-timestamp entries in non-bucketed mode.
    static constexpr size_t MAX_UNBUCKETED_STATES = 10'000'000;

    explicit BlockAggregator(uint64_t interval)
        : interval_(interval), method_(AggregationMethod::AVG), methodAware_(false) {}

    BlockAggregator(uint64_t interval, AggregationMethod method)
        : interval_(interval), method_(method), methodAware_(true) {}

    // Constructor overload that pre-allocates the bucket map when all three
    // parameters are known at construction time.
    //
    // Buckets are epoch-aligned (floor(ts / interval) * interval) and endTime
    // is inclusive, so the bucket count is the number of epoch buckets the
    // range [startTime, endTime] overlaps — NOT ceil(range / interval).  A
    // misaligned range shorter than one interval can still overlap TWO epoch
    // buckets; computing the count from the range length used to engage the
    // single-bucket optimisation in that case and collapsed distinct epoch
    // buckets into one (stamped floor(startTime / interval)).
    BlockAggregator(uint64_t interval, uint64_t startTime, uint64_t endTime,
                    AggregationMethod method = AggregationMethod::AVG, bool methodAware = false)
        : interval_(interval), method_(method), methodAware_(methodAware) {
        if (interval_ > 0 && endTime >= startTime) {
            const uint64_t firstBucket = (startTime / interval_) * interval_;
            const uint64_t lastBucket = (endTime / interval_) * interval_;
            uint64_t bucketCount = (lastBucket - firstBucket) / interval_ + 1;
            if (bucketCount > MAX_PREALLOCATED_BUCKETS) {
                bucketCount = MAX_PREALLOCATED_BUCKETS;
            }
            bucketStates_.reserve(static_cast<size_t>(bucketCount));
            // Single-bucket optimisation: pre-insert the only bucket and cache
            // a direct pointer, eliminating per-point division + hash lookup.
            // Valid only when the whole [startTime, endTime] range maps to a
            // single epoch bucket; add* methods additionally verify that each
            // point actually belongs to this bucket (see inSingleBucket()) so
            // that bucketing stays a pure function of the data even if a
            // caller feeds points outside the constructed range.
            if (bucketCount == 1) {
                singleBucketKey_ = firstBucket;
                auto [it, _] = bucketStates_.emplace(singleBucketKey_, AggregationState{});
                singleBucketState_ = &it->second;
            }
        }
    }

    // Enable fold-to-single-state mode for non-bucketed streaming aggregation.
    // When active (interval_ == 0), addPoint/addPoints fold directly into a single
    // AggregationState instead of materialising raw (timestamp, value) vectors.
    // collectRaw is always false for fold mode (raw collection is only for
    // EXACT_MEDIAN, which never uses fold).
    void enableFoldToSingleState() {
        foldToSingleState_ = true;
        singleState_.collectRaw = false;
    }

    // Check if a block's stats can be used without decoding.
    // Returns true when all points in the block would go to the same
    // aggregation target (single bucket, fold mode, or block fits in one bucket)
    // AND the aggregation method doesn't require per-point computation.
    // hasExtendedStats: true when block has M2/firstValue/latestValue (Float series).
    bool canUseBlockStats(uint64_t blockMinTime, uint64_t blockMaxTime, bool hasExtendedStats = false) const {
        if (methodAware_) {
            switch (method_) {
                // MEDIAN (t-digest) and EXACT_MEDIAN both require individual values.
                // Block-level stats cannot substitute for per-value processing.
                case AggregationMethod::MEDIAN:
                case AggregationMethod::EXACT_MEDIAN:
                    return false;
                // STDDEV/STDVAR need Welford M2; LATEST/FIRST need actual values.
                // These can use block stats only when extended stats are available.
                case AggregationMethod::STDDEV:
                case AggregationMethod::STDVAR:
                case AggregationMethod::LATEST:
                case AggregationMethod::FIRST:
                    if (!hasExtendedStats)
                        return false;
                    break;
                default:
                    break;
            }
        }
        if (interval_ == 0)
            return foldToSingleState_;
        // Block must fit within a single epoch bucket.  This applies in
        // single-bucket mode too: a block may legitimately span a range
        // outside the constructed [startTime, endTime] window (e.g. memory
        // fallback data appended after a TSM-only range was used for
        // construction), so it cannot be blindly merged into the cached
        // single bucket.
        uint64_t minBucket = (blockMinTime / interval_) * interval_;
        uint64_t maxBucket = (blockMaxTime / interval_) * interval_;
        return minBucket == maxBucket;
    }

    // Merge pre-computed block statistics without decoding the block.
    // Only valid when canUseBlockStats() returns true, the entire block
    // falls within the query time range, and there are no tombstones.
    void addBlockStats(double sum, double bmin, double bmax, uint32_t count, uint64_t minTime, uint64_t maxTime,
                       double m2 = 0.0, double firstValue = 0.0, double latestValue = 0.0) {
        pointCount_ += count;
        AggregationState blockState;
        blockState.sum = sum;
        blockState.min = bmin;
        blockState.max = bmax;
        blockState.count = count;
        blockState.latestTimestamp = maxTime;
        blockState.firstTimestamp = minTime;
        blockState.m2 = m2;
        blockState.mean = (count > 0) ? sum / count : 0.0;
        blockState.latest = latestValue;
        blockState.first = firstValue;

        auto doMerge = [&](AggregationState& target) {
            if (methodAware_) {
                target.mergeForMethod(blockState, method_);
            } else {
                target.merge(blockState);
            }
        };

        if (interval_ == 0) {
            // Non-bucketed fold mode (canUseBlockStats gates on foldToSingleState_)
            doMerge(singleState_);
            return;
        }
        // Bucketed: block fits in a single epoch bucket (verified by
        // canUseBlockStats).  Route by the block's own bucket — the cached
        // single-bucket pointer is only a shortcut when the block actually
        // belongs to that bucket.
        uint64_t bucket = (minTime / interval_) * interval_;
        if (singleBucketState_ && bucket == singleBucketKey_) {
            doMerge(*singleBucketState_);
        } else {
            doMerge(bucketStates_[bucket]);
        }
    }

    // Move out the single accumulated state (fold mode).
    AggregationState takeSingleState() { return std::move(singleState_); }

    // Returns true when only point counts are needed (COUNT method), enabling
    // the caller to skip value decoding entirely.
    bool isCountOnly() const { return methodAware_ && method_ == AggregationMethod::COUNT; }

    // Add timestamps without values (COUNT optimization — skips value decode).
    // Increments count for each timestamp's bucket without touching other accumulators.
    void addTimestampsOnly(const std::vector<uint64_t>& timestamps) {
        pointCount_ += timestamps.size();
        if (interval_ == 0) {
            if (foldToSingleState_) {
                singleState_.count += timestamps.size();
                return;
            }
            // Non-bucketed: store timestamps with zero values for result pipeline
            timestamps_.insert(timestamps_.end(), timestamps.begin(), timestamps.end());
            rawValues_.resize(rawValues_.size() + timestamps.size(), 0.0);
        } else if (singleBucketState_ && !timestamps.empty() && inSingleBucket(timestamps.front()) &&
                   inSingleBucket(timestamps.back())) {
            singleBucketState_->count += timestamps.size();
        } else {
            // Timestamps are monotonic within a block: batch consecutive
            // same-bucket runs into one hash lookup + bulk count add.
            const size_t n = timestamps.size();
            size_t i = 0;
            while (i < n) {
                uint64_t bucket = (timestamps[i] / interval_) * interval_;
                uint64_t nextBucket = bucket + interval_;
                size_t j = i + 1;
                while (j < n && timestamps[j] < nextBucket) {
                    ++j;
                }
                bucketStates_[bucket].count += j - i;
                i = j;
            }
        }
    }

    // Add a single data point.
    void addPoint(uint64_t timestamp, double value) {
        ++pointCount_;
        if (interval_ == 0) {
            if (foldToSingleState_) {
                addToState(singleState_, value, timestamp);
                return;
            }
            if (timestamps_.size() >= MAX_UNBUCKETED_STATES) [[unlikely]] {
                throw std::runtime_error("Non-bucketed aggregation exceeded " + std::to_string(MAX_UNBUCKETED_STATES) +
                                         " states; use an aggregationInterval to reduce cardinality");
            }
            timestamps_.push_back(timestamp);
            rawValues_.push_back(value);
        } else if (singleBucketState_ && inSingleBucket(timestamp)) {
            addToState(*singleBucketState_, value, timestamp);
        } else {
            uint64_t bucket = (timestamp / interval_) * interval_;
            addToState(bucketStates_[bucket], value, timestamp);
        }
    }

    // Add a subrange of a batch (zero-copy from MemoryStore).
    void addPointsRange(const std::vector<uint64_t>& timestamps, const std::vector<double>& values, size_t startIdx,
                        size_t endIdx) {
        const size_t n = endIdx - startIdx;
        pointCount_ += n;
        if (n == 0)
            return;

        if (interval_ == 0) {
            if (foldToSingleState_) {
                if (methodAware_ && n >= 4) {
                    addPointsSIMDFoldRange(timestamps, values, startIdx, endIdx, singleState_);
                } else {
                    for (size_t i = startIdx; i < endIdx; ++i) {
                        addToState(singleState_, values[i], timestamps[i]);
                    }
                }
                return;
            }
            timestamps_.insert(timestamps_.end(), timestamps.begin() + startIdx, timestamps.begin() + endIdx);
            rawValues_.insert(rawValues_.end(), values.begin() + startIdx, values.begin() + endIdx);
        } else if (singleBucketState_ && inSingleBucket(timestamps[startIdx]) && inSingleBucket(timestamps[endIdx - 1])) {
            if (methodAware_ && n >= 4) {
                addPointsSIMDFoldRange(timestamps, values, startIdx, endIdx, *singleBucketState_);
            } else {
                for (size_t i = startIdx; i < endIdx; ++i) {
                    addToState(*singleBucketState_, values[i], timestamps[i]);
                }
            }
        } else {
            foldSortedMultiBucket(timestamps, values, startIdx, endIdx);
        }
    }

    // Add a batch of decoded points (contiguous arrays).
    void addPoints(const std::vector<uint64_t>& timestamps, const std::vector<double>& values) {
        const size_t n = timestamps.size();
        pointCount_ += n;
        if (interval_ == 0) {
            if (foldToSingleState_) {
                if (methodAware_ && n >= 4) {
                    addPointsSIMDFold(timestamps, values);
                } else {
                    for (size_t i = 0; i < n; ++i) {
                        addToState(singleState_, values[i], timestamps[i]);
                    }
                }
                return;
            }
            if (timestamps_.size() + n > MAX_UNBUCKETED_STATES) [[unlikely]] {
                throw std::runtime_error("Non-bucketed aggregation exceeded " + std::to_string(MAX_UNBUCKETED_STATES) +
                                         " states; use an aggregationInterval to reduce cardinality");
            }
            // Store raw (timestamp, value) pairs — 16 bytes/point vs 96 bytes
            // with AggregationState. States are constructed lazily at merge time.
            timestamps_.insert(timestamps_.end(), timestamps.begin(), timestamps.end());
            rawValues_.insert(rawValues_.end(), values.begin(), values.end());
        } else if (singleBucketState_ && n > 0 && inSingleBucket(timestamps.front()) &&
                   inSingleBucket(timestamps.back())) {
            // Single-bucket fast path: skip division + hash lookup per point.
            if (methodAware_ && n >= 4) {
                addPointsSIMDFold(timestamps, values, singleBucketState_);
            } else {
                auto* state = singleBucketState_;
                for (size_t i = 0; i < n; ++i) {
                    addToState(*state, values[i], timestamps[i]);
                }
            }
        } else {
            foldSortedMultiBucket(timestamps, values, 0, n);
        }
    }

    size_t pointCount() const { return pointCount_; }

    // Move out bucket states (interval > 0).
    std::unordered_map<uint64_t, AggregationState> takeBucketStates() {
        // The single-bucket optimisation pre-inserts its bucket at
        // construction; if no point actually landed in it (e.g. all data fell
        // into other epoch buckets or there was no data at all), drop the
        // empty state so it doesn't surface as a bogus bucket downstream.
        if (singleBucketState_ && singleBucketState_->count == 0) {
            bucketStates_.erase(singleBucketKey_);
        }
        singleBucketState_ = nullptr;
        return std::move(bucketStates_);
    }

    // Move out per-timestamp data (interval == 0).
    std::vector<uint64_t> takeTimestamps() { return std::move(timestamps_); }
    std::vector<double> takeValues() { return std::move(rawValues_); }

    // Sort per-timestamp data. Required after parallel_for_each on batches
    // because batch completion order is non-deterministic.
    void sortTimestamps() {
        if (timestamps_.size() <= 1)
            return;
        // Check if already sorted (common case: single batch or in-order completion)
        bool sorted = true;
        for (size_t i = 1; i < timestamps_.size(); ++i) {
            if (timestamps_[i] < timestamps_[i - 1]) {
                sorted = false;
                break;
            }
        }
        if (sorted)
            return;
        // Index permutation sort to keep timestamps and values in sync
        std::vector<size_t> indices(timestamps_.size());
        std::iota(indices.begin(), indices.end(), 0);
        std::sort(indices.begin(), indices.end(),
                  [this](size_t a, size_t b) { return timestamps_[a] < timestamps_[b]; });
        std::vector<uint64_t> sortedTs(timestamps_.size());
        std::vector<double> sortedVals(rawValues_.size());
        for (size_t i = 0; i < indices.size(); ++i) {
            sortedTs[i] = timestamps_[indices[i]];
            sortedVals[i] = rawValues_[indices[i]];
        }
        timestamps_ = std::move(sortedTs);
        rawValues_ = std::move(sortedVals);
    }

private:
    // Multi-bucket fold over sorted timestamps: batch consecutive timestamps in
    // the same bucket, so each run costs one division + one hash lookup + one
    // SIMD fold instead of per-point division/hash/switch. Timestamps are
    // monotonic within a TSM block and within MemoryStore batches (callers
    // binary-search them), which is the precondition for run detection.
    void foldSortedMultiBucket(const std::vector<uint64_t>& timestamps, const std::vector<double>& values,
                               size_t startIdx, size_t endIdx) {
        size_t i = startIdx;
        while (i < endIdx) {
            uint64_t bucket = (timestamps[i] / interval_) * interval_;
            uint64_t nextBucket = bucket + interval_;
            size_t j = i + 1;
            while (j < endIdx && timestamps[j] < nextBucket) {
                ++j;
            }
            auto& state = bucketStates_[bucket];
            if (methodAware_ && (j - i) >= 4) {
                addPointsSIMDFoldRange(timestamps, values, i, j, state);
            } else {
                for (size_t k = i; k < j; ++k) {
                    addToState(state, values[k], timestamps[k]);
                }
            }
            i = j;
        }
    }

    // True when a timestamp belongs to the cached single bucket.  Unsigned
    // wrap-around makes timestamps below singleBucketKey_ compare huge, so a
    // single comparison covers both bounds.  Callers must only use this when
    // singleBucketState_ is set (implies interval_ > 0).
    bool inSingleBucket(uint64_t timestamp) const { return timestamp - singleBucketKey_ < interval_; }

    // Dispatch to method-aware or generic addValue based on construction.
    void addToState(AggregationState& state, double value, uint64_t timestamp) {
        if (methodAware_) {
            state.addValueForMethod(value, timestamp, method_);
        } else {
            state.addValue(value, timestamp);
        }
    }

    // SIMD batch fold: process an entire batch using vectorised reductions
    // for methods that support it (SUM/AVG/MIN/MAX/COUNT/SPREAD/STDDEV/STDVAR).
    // Falls back to scalar for MEDIAN/EXACT_MEDIAN.
    //
    // NOTE: The SIMD paths for AVG/SUM/MIN/MAX/COUNT/SPREAD only update the
    // accumulators they need (sum, count, min, max). STDDEV/STDVAR maintain
    // mean/m2 via a two-pass batch variance + Chan's parallel Welford merge.
    //
    // NaN handling (docs/nan_policy.md): every method whose OUTPUT depends on
    // count or values detects NaN (NaN SIMD sum / NaN min/max / boundary
    // check) and falls back to the scalar NaN-skipping fold. For MIN/MAX/
    // SPREAD/LATEST/FIRST fast paths, interior NaN may still be included in
    // state.count — count is not part of those methods' results (it only
    // gates emptiness), so this is unobservable.
    // After updating sum/count we recompute state.mean = state.sum / state.count
    // so that any downstream code reading mean (e.g. mergeForMethod) sees a
    // consistent value.
    void addPointsSIMDFold(const std::vector<uint64_t>& timestamps, const std::vector<double>& values,
                           AggregationState* state = nullptr) {
        addPointsSIMDFoldRange(timestamps, values, 0, timestamps.size(), state ? *state : singleState_);
    }

    void addPointsSIMDFoldRange(const std::vector<uint64_t>& timestamps, const std::vector<double>& values,
                                size_t begin, size_t end, AggregationState& state) {
        const size_t n = end - begin;
        const double* vdata = values.data() + begin;
        const uint64_t* tdata = timestamps.data() + begin;

        // NaN CAN appear in decoded data (NaN is stored verbatim; canonical
        // policy: NaN = missing, skipped by every method — docs/nan_policy.md).
        // Cheap prefilter: if the first element is NaN, bail to the scalar
        // path which correctly skips NaN via addValueForMethod(). Interior
        // NaN is caught per-method below (NaN sum / NaN min/max / boundary
        // checks) with the same scalar fallback.
        if (n > 0 && std::isnan(vdata[0])) [[unlikely]] {
            for (size_t i = begin; i < end; ++i) {
                addToState(state, values[i], timestamps[i]);
            }
            return;
        }

        switch (method_) {
            case AggregationMethod::AVG:
            case AggregationMethod::SUM: {
                double simdSum = simd::SimdAggregator::calculateSum(vdata, n);
                // Defense-in-depth: if SIMD sum is NaN (interior NaN values),
                // fall back to scalar which skips NaN correctly.
                if (std::isnan(simdSum)) [[unlikely]] {
                    for (size_t i = begin; i < end; ++i) {
                        addToState(state, values[i], timestamps[i]);
                    }
                } else {
                    // Kahan compensated addition of SIMD sum to preserve precision.
                    // Non-finite guard: with a ±Inf sum the compensation term
                    // degenerates to NaN and would poison later additions,
                    // turning Inf results into NaN (canonical: Inf propagates).
                    double y = simdSum - state.sumCompensation;
                    double t = state.sum + y;
                    state.sumCompensation = std::isfinite(t) ? (t - state.sum) - y : 0.0;
                    state.sum = t;
                    state.count += n;
                    // Keep mean consistent for downstream merge operations.
                    state.mean = (state.sum + state.sumCompensation) / static_cast<double>(state.count);
                    // Track first/latest so getTimestamp() returns valid values.
                    if (tdata[0] < state.firstTimestamp) {
                        state.firstTimestamp = tdata[0];
                        state.first = vdata[0];
                    }
                    if (tdata[n - 1] >= state.latestTimestamp) {
                        state.latestTimestamp = tdata[n - 1];
                        state.latest = vdata[n - 1];
                    }
                }
                break;
            }
            case AggregationMethod::COUNT: {
                // COUNT counts only non-NaN values (NaN = missing data). A NaN
                // SIMD sum means NaN is present (or an Inf/-Inf mix) — fall
                // back to the scalar NaN-skipping fold. One fused pass; the
                // NaN-free common case still counts in O(1) after the check.
                if (std::isnan(simd::SimdAggregator::calculateSum(vdata, n))) [[unlikely]] {
                    for (size_t i = begin; i < end; ++i) {
                        addToState(state, values[i], timestamps[i]);
                    }
                    break;
                }
                state.count += n;
                if (n > 0) {
                    if (tdata[0] < state.firstTimestamp) {
                        state.firstTimestamp = tdata[0];
                        state.first = vdata[0];
                    }
                    if (tdata[n - 1] >= state.latestTimestamp) {
                        state.latestTimestamp = tdata[n - 1];
                        state.latest = vdata[n - 1];
                    }
                }
                break;
            }
            case AggregationMethod::MIN: {
                double simdMin = simd::SimdAggregator::calculateMin(vdata, n);
                if (std::isnan(simdMin)) [[unlikely]] {
                    for (size_t i = begin; i < end; ++i) {
                        addToState(state, values[i], timestamps[i]);
                    }
                } else {
                    state.min = std::min(state.min, simdMin);
                    state.count += n;
                    if (tdata[0] < state.firstTimestamp) {
                        state.firstTimestamp = tdata[0];
                        state.first = vdata[0];
                    }
                    if (tdata[n - 1] >= state.latestTimestamp) {
                        state.latestTimestamp = tdata[n - 1];
                        state.latest = vdata[n - 1];
                    }
                }
                break;
            }
            case AggregationMethod::MAX: {
                double simdMax = simd::SimdAggregator::calculateMax(vdata, n);
                if (std::isnan(simdMax)) [[unlikely]] {
                    for (size_t i = begin; i < end; ++i) {
                        addToState(state, values[i], timestamps[i]);
                    }
                } else {
                    state.max = std::max(state.max, simdMax);
                    state.count += n;
                    if (tdata[0] < state.firstTimestamp) {
                        state.firstTimestamp = tdata[0];
                        state.first = vdata[0];
                    }
                    if (tdata[n - 1] >= state.latestTimestamp) {
                        state.latestTimestamp = tdata[n - 1];
                        state.latest = vdata[n - 1];
                    }
                }
                break;
            }
            case AggregationMethod::SPREAD: {
                double simdMin = simd::SimdAggregator::calculateMin(vdata, n);
                double simdMax = simd::SimdAggregator::calculateMax(vdata, n);
                if (std::isnan(simdMin) || std::isnan(simdMax)) [[unlikely]] {
                    for (size_t i = begin; i < end; ++i) {
                        addToState(state, values[i], timestamps[i]);
                    }
                } else {
                    state.min = std::min(state.min, simdMin);
                    state.max = std::max(state.max, simdMax);
                    state.count += n;
                    if (tdata[0] < state.firstTimestamp) {
                        state.firstTimestamp = tdata[0];
                        state.first = vdata[0];
                    }
                    if (tdata[n - 1] >= state.latestTimestamp) {
                        state.latestTimestamp = tdata[n - 1];
                        state.latest = vdata[n - 1];
                    }
                }
                break;
            }
            case AggregationMethod::LATEST:
                // Timestamps are monotonically increasing within a TSM block,
                // so the last element is always the latest. If it is NaN the
                // canonical answer is the last non-NaN value — scalar fallback
                // (the entry prefilter above only checks vdata[0]).
                if (n > 0 && std::isnan(vdata[n - 1])) [[unlikely]] {
                    for (size_t i = begin; i < end; ++i) {
                        addToState(state, values[i], timestamps[i]);
                    }
                    break;
                }
                if (n > 0 && tdata[n - 1] >= state.latestTimestamp) {
                    state.latest = vdata[n - 1];
                    state.latestTimestamp = tdata[n - 1];
                }
                state.count += n;
                break;
            case AggregationMethod::FIRST:
                // Monotonic timestamps: first element is the earliest.
                // Use strict < to match scalar addValueForMethod behavior.
                if (n > 0 && tdata[0] < state.firstTimestamp) {
                    state.first = vdata[0];
                    state.firstTimestamp = tdata[0];
                }
                state.count += n;
                break;
            case AggregationMethod::STDDEV:
            case AggregationMethod::STDVAR: {
                // Two-pass SIMD variance: batch mean via calculateSum, then batch
                // M2 via calculateVariance, merged with Chan's parallel Welford
                // formula. Numerically equivalent-or-better vs per-point Welford.
                double simdSum = simd::SimdAggregator::calculateSum(vdata, n);
                if (std::isnan(simdSum)) [[unlikely]] {
                    for (size_t i = begin; i < end; ++i) {
                        addToState(state, values[i], timestamps[i]);
                    }
                } else {
                    const double batchMean = simdSum / static_cast<double>(n);
                    // calculateVariance returns population variance (M2/n).
                    const double batchM2 =
                        simd::SimdAggregator::calculateVariance(vdata, n, batchMean) * static_cast<double>(n);
                    if (state.count == 0) {
                        state.mean = batchMean;
                        state.m2 = batchM2;
                    } else {
                        const double delta = batchMean - state.mean;
                        const double totalCount = static_cast<double>(state.count) + static_cast<double>(n);
                        state.m2 += batchM2 + delta * delta *
                                                  (static_cast<double>(state.count) * static_cast<double>(n)) /
                                                  totalCount;
                        state.mean += delta * (static_cast<double>(n) / totalCount);
                    }
                    state.count += n;
                    if (tdata[0] < state.firstTimestamp) {
                        state.firstTimestamp = tdata[0];
                        state.first = vdata[0];
                    }
                    if (tdata[n - 1] >= state.latestTimestamp) {
                        state.latestTimestamp = tdata[n - 1];
                        state.latest = vdata[n - 1];
                    }
                }
                break;
            }
            default:
                // MEDIAN, EXACT_MEDIAN, etc. — fall back to scalar per-value
                for (size_t i = begin; i < end; ++i) {
                    addToState(state, values[i], timestamps[i]);
                }
                break;
        }
    }

    uint64_t interval_;
    AggregationMethod method_;
    bool methodAware_;
    // Bucketed mode
    std::unordered_map<uint64_t, AggregationState> bucketStates_;
    // Non-bucketed mode: compact raw storage (16 bytes/point)
    std::vector<uint64_t> timestamps_;
    std::vector<double> rawValues_;
    // Single-bucket optimisation: cached pointer to the only bucket entry,
    // avoiding per-point integer division and hash map lookup.
    // INVARIANT: singleBucketState_ points into bucketStates_. std::unordered_map
    // is node-based, so inserting additional buckets through the multi-bucket
    // path (points outside [singleBucketKey_, singleBucketKey_ + interval_),
    // see inSingleBucket()) never invalidates this pointer — rehashing moves
    // buckets, not nodes.
    AggregationState* singleBucketState_ = nullptr;
    uint64_t singleBucketKey_ = 0;
    // Non-bucketed fold mode: single accumulated state (streaming aggregation)
    bool foldToSingleState_ = false;
    AggregationState singleState_;
    size_t pointCount_ = 0;
};

// Lightweight result from pushdown aggregation, before being wrapped into
// a PartialAggregationResult by the caller.
struct PushdownResult {
    // For bucketed (interval > 0):
    std::unordered_map<uint64_t, AggregationState> bucketStates;
    // For non-bucketed (interval == 0) — raw values (16 bytes/point):
    std::vector<uint64_t> sortedTimestamps;
    std::vector<double> sortedValues;
    // For non-bucketed streaming aggregation (interval == 0, streamable method):
    std::optional<AggregationState> aggregatedState;
    size_t totalPoints = 0;
};

}  // namespace timestar
