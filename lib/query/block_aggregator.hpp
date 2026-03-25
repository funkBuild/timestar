#pragma once

#include "aggregator.hpp"
#include "simd_aggregator.hpp"

#include <algorithm>
#include <cassert>
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
    // parameters are known at construction time.  Bucket count is computed as
    // ceil((endTime - startTime) / interval), capped at MAX_PREALLOCATED_BUCKETS
    // to prevent excessive allocations for pathological inputs.
    BlockAggregator(uint64_t interval, uint64_t startTime, uint64_t endTime,
                    AggregationMethod method = AggregationMethod::AVG, bool methodAware = false)
        : interval_(interval), method_(method), methodAware_(methodAware) {
        if (interval_ > 0 && endTime > startTime) {
            uint64_t range = endTime - startTime;
            // Overflow-safe ceiling division
            uint64_t bucketCount = range / interval_ + (range % interval_ != 0 ? 1 : 0);
            if (bucketCount > MAX_PREALLOCATED_BUCKETS) {
                bucketCount = MAX_PREALLOCATED_BUCKETS;
            }
            bucketStates_.reserve(static_cast<size_t>(bucketCount));
            // Single-bucket optimisation: pre-insert the only bucket and cache
            // a direct pointer, eliminating per-point division + hash lookup.
            if (bucketCount == 1) {
                singleBucketKey_ = (startTime / interval_) * interval_;
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
        if (singleBucketState_)
            return true;
        // Multi-bucket: block must fit within a single bucket
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

        assert(!singleBucketState_ || (minTime / interval_) * interval_ == singleBucketKey_);
        if (singleBucketState_) {
            doMerge(*singleBucketState_);
        } else if (interval_ == 0 && foldToSingleState_) {
            doMerge(singleState_);
        } else {
            // Multi-bucket: block fits in a single bucket (verified by canUseBlockStats)
            uint64_t bucket = (minTime / interval_) * interval_;
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
        } else if (singleBucketState_) {
            singleBucketState_->count += timestamps.size();
        } else {
            for (size_t i = 0; i < timestamps.size(); ++i) {
                uint64_t bucket = (timestamps[i] / interval_) * interval_;
                bucketStates_[bucket].count++;
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
        } else if (singleBucketState_) {
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
        } else if (singleBucketState_) {
            if (methodAware_ && n >= 4) {
                addPointsSIMDFoldRange(timestamps, values, startIdx, endIdx, *singleBucketState_);
            } else {
                for (size_t i = startIdx; i < endIdx; ++i) {
                    addToState(*singleBucketState_, values[i], timestamps[i]);
                }
            }
        } else {
            for (size_t i = startIdx; i < endIdx; ++i) {
                uint64_t bucket = (timestamps[i] / interval_) * interval_;
                addToState(bucketStates_[bucket], values[i], timestamps[i]);
            }
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
        } else if (singleBucketState_) {
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
            // Multi-bucket: batch consecutive timestamps in the same bucket.
            // Timestamps are monotonic within a block, so we compute the next
            // bucket boundary once and compare directly — avoids division per point.
            size_t i = 0;
            while (i < n) {
                uint64_t bucket = (timestamps[i] / interval_) * interval_;
                uint64_t nextBucket = bucket + interval_;
                size_t j = i + 1;
                while (j < n && timestamps[j] < nextBucket) {
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
    }

    size_t pointCount() const { return pointCount_; }

    // Move out bucket states (interval > 0).
    std::unordered_map<uint64_t, AggregationState> takeBucketStates() {
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
    // Dispatch to method-aware or generic addValue based on construction.
    void addToState(AggregationState& state, double value, uint64_t timestamp) {
        if (methodAware_) {
            state.addValueForMethod(value, timestamp, method_);
        } else {
            state.addValue(value, timestamp);
        }
    }

    // SIMD batch fold: process an entire batch using vectorised reductions
    // for methods that support it (SUM/AVG/MIN/MAX/COUNT/SPREAD).
    // Falls back to scalar for LATEST/FIRST/STDDEV/MEDIAN.
    //
    // NOTE: The SIMD paths for AVG/SUM/MIN/MAX/COUNT/SPREAD only update the
    // accumulators they need (sum, count, min, max). They do NOT update
    // singleState_.m2 or singleState_.mean via Welford's algorithm — those
    // fields are only valid when method_ is STDDEV/STDVAR, which always takes
    // the scalar fallback path. After updating sum/count we recompute
    // state.mean = state.sum / state.count so that any downstream code
    // reading mean (e.g. mergeForMethod) sees a consistent value.
    void addPointsSIMDFold(const std::vector<uint64_t>& timestamps, const std::vector<double>& values,
                           AggregationState* state = nullptr) {
        addPointsSIMDFoldRange(timestamps, values, 0, timestamps.size(), state ? *state : singleState_);
    }

    void addPointsSIMDFoldRange(const std::vector<uint64_t>& timestamps, const std::vector<double>& values,
                                size_t begin, size_t end, AggregationState& state) {
        const size_t n = end - begin;
        const double* vdata = values.data() + begin;
        const uint64_t* tdata = timestamps.data() + begin;

        // INVARIANT: Decoded TSM block data is NaN-free (NaN filtered during write).
        // Defense-in-depth: if the first element is NaN (e.g. corrupt data), bail to
        // the scalar path which correctly skips NaN via addValueForMethod().
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
                    // Kahan compensated addition of SIMD sum to preserve precision
                    double y = simdSum - state.sumCompensation;
                    double t = state.sum + y;
                    state.sumCompensation = (t - state.sum) - y;
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
            case AggregationMethod::COUNT:
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
                // so the last element is always the latest.
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
            default:
                // STDDEV, MEDIAN, EXACT_MEDIAN, etc. — fall back to scalar per-value
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
    // INVARIANT: singleBucketState_ points into bucketStates_ (std::unordered_map,
    // node-stable for existing entries). Only valid while no insertions to
    // bucketStates_ occur through the multi-bucket path. The single-bucket
    // optimization (bucketCount==1) ensures this: all inserts go through
    // singleBucketState_ directly, never through bucketStates_[key].
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
