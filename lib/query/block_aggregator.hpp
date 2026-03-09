#ifndef BLOCK_AGGREGATOR_H_INCLUDED
#define BLOCK_AGGREGATOR_H_INCLUDED

#include "aggregator.hpp"
#include <algorithm>
#include <cstdint>
#include <numeric>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace timestar {

// Stateful sink that folds decoded (timestamp, value) pairs directly into
// AggregationState(s), avoiding intermediate vector materialisation.
//
// Bucketed mode (interval > 0): values are folded into time-bucket states,
// reducing N data points to M buckets.
// Non-bucketed mode (interval == 0): per-timestamp AggregationStates are
// stored directly, avoiding the TSMResult/QueryResult/SeriesResult pipeline
// while preserving individual timestamps for downstream merge.
class BlockAggregator {
public:
    // Maximum number of buckets to pre-allocate (guards against pathological inputs
    // such as a tiny interval over a very large time range).
    static constexpr size_t MAX_PREALLOCATED_BUCKETS = 100'000;

    // Maximum number of per-timestamp states in non-bucketed mode.
    // Each AggregationState is ~96 bytes, so 10M states ≈ ~1GB.
    static constexpr size_t MAX_UNBUCKETED_STATES = 10'000'000;

    BlockAggregator(uint64_t interval) : interval_(interval) {}

    // Constructor overload that pre-allocates the bucket map when all three
    // parameters are known at construction time.  Bucket count is computed as
    // ceil((endTime - startTime) / interval), capped at MAX_PREALLOCATED_BUCKETS
    // to prevent excessive allocations for pathological inputs.
    BlockAggregator(uint64_t interval, uint64_t startTime, uint64_t endTime)
        : interval_(interval)
    {
        if (interval_ > 0 && endTime > startTime) {
            uint64_t range = endTime - startTime;
            // Ceiling division: (range + interval - 1) / interval
            uint64_t bucketCount = (range + interval_ - 1) / interval_;
            if (bucketCount > MAX_PREALLOCATED_BUCKETS) {
                bucketCount = MAX_PREALLOCATED_BUCKETS;
            }
            bucketStates_.reserve(static_cast<size_t>(bucketCount));
        }
    }

    // Add a single data point.
    void addPoint(uint64_t timestamp, double value) {
        ++pointCount_;
        if (interval_ == 0) {
            if (timestamps_.size() >= MAX_UNBUCKETED_STATES) [[unlikely]] {
                throw std::runtime_error(
                    "Non-bucketed aggregation exceeded " +
                    std::to_string(MAX_UNBUCKETED_STATES) +
                    " states; use an aggregationInterval to reduce cardinality");
            }
            timestamps_.push_back(timestamp);
            AggregationState s;
            s.addValue(value, timestamp);
            states_.push_back(s);
        } else {
            uint64_t bucket = (timestamp / interval_) * interval_;
            bucketStates_[bucket].addValue(value, timestamp);
        }
    }

    // Add a batch of decoded points (contiguous arrays).
    void addPoints(const std::vector<uint64_t>& timestamps,
                   const std::vector<double>& values) {
        pointCount_ += timestamps.size();
        if (interval_ == 0) {
            if (timestamps_.size() + timestamps.size() > MAX_UNBUCKETED_STATES) [[unlikely]] {
                throw std::runtime_error(
                    "Non-bucketed aggregation exceeded " +
                    std::to_string(MAX_UNBUCKETED_STATES) +
                    " states; use an aggregationInterval to reduce cardinality");
            }
            timestamps_.reserve(timestamps_.size() + timestamps.size());
            states_.reserve(states_.size() + timestamps.size());
            for (size_t i = 0; i < timestamps.size(); ++i) {
                timestamps_.push_back(timestamps[i]);
                AggregationState s;
                s.addValue(values[i], timestamps[i]);
                states_.push_back(s);
            }
        } else {
            for (size_t i = 0; i < timestamps.size(); ++i) {
                uint64_t bucket = (timestamps[i] / interval_) * interval_;
                bucketStates_[bucket].addValue(values[i], timestamps[i]);
            }
        }
    }

    size_t pointCount() const { return pointCount_; }

    // Move out bucket states (interval > 0).
    std::unordered_map<uint64_t, AggregationState> takeBucketStates() { return std::move(bucketStates_); }

    // Move out per-timestamp data (interval == 0).
    std::vector<uint64_t> takeTimestamps() { return std::move(timestamps_); }
    std::vector<AggregationState> takeStates() { return std::move(states_); }

    // Sort per-timestamp data. Required after parallel_for_each on batches
    // because batch completion order is non-deterministic.
    void sortTimestamps() {
        if (timestamps_.size() <= 1) return;
        // Check if already sorted (common case: single batch or in-order completion)
        bool sorted = true;
        for (size_t i = 1; i < timestamps_.size(); ++i) {
            if (timestamps_[i] < timestamps_[i - 1]) {
                sorted = false;
                break;
            }
        }
        if (sorted) return;
        // Index permutation sort to keep timestamps and states in sync
        std::vector<size_t> indices(timestamps_.size());
        std::iota(indices.begin(), indices.end(), 0);
        std::sort(indices.begin(), indices.end(),
            [this](size_t a, size_t b) { return timestamps_[a] < timestamps_[b]; });
        std::vector<uint64_t> sortedTs(timestamps_.size());
        std::vector<AggregationState> sortedSt(states_.size());
        for (size_t i = 0; i < indices.size(); ++i) {
            sortedTs[i] = timestamps_[indices[i]];
            sortedSt[i] = std::move(states_[indices[i]]);
        }
        timestamps_ = std::move(sortedTs);
        states_ = std::move(sortedSt);
    }

private:
    uint64_t interval_;
    // Bucketed mode
    std::unordered_map<uint64_t, AggregationState> bucketStates_;
    // Non-bucketed mode
    std::vector<uint64_t> timestamps_;
    std::vector<AggregationState> states_;
    size_t pointCount_ = 0;
};

// Lightweight result from pushdown aggregation, before being wrapped into
// a PartialAggregationResult by the caller.
struct PushdownResult {
    // For bucketed (interval > 0):
    std::unordered_map<uint64_t, AggregationState> bucketStates;
    // For non-bucketed (interval == 0):
    std::vector<uint64_t> sortedTimestamps;
    std::vector<AggregationState> sortedStates;
    size_t totalPoints = 0;
};

} // namespace timestar

#endif // BLOCK_AGGREGATOR_H_INCLUDED
