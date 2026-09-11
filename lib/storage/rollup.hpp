#pragma once

#include "query_parser.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <vector>

// Persisted with a compacted point. method==0 denotes an ordinary raw point.
// Counts distinguish an aggregate NaN from a missing raw sample. The original
// latest timestamp is independent of the bucket label. Integer sums never pass
// through double, including when the displayed integer average is truncated.
struct RollupState {
    uint64_t interval = 0;
    uint64_t count = 0;
    uint64_t latestTimestamp = 0;
    double sum = 0;
    double compensation = 0;
    __int128_t integerSum = 0;
    uint8_t method = 0;

    bool folded() const { return method != 0; }
    timestar::AggregationMethod aggregation() const { return static_cast<timestar::AggregationMethod>(method - 1); }
};

template <typename T>
class RollupAccumulator {
    using Method = timestar::AggregationMethod;
    T value_{};
    bool initialized_ = false;

    void addSum(double value) {
        const double y = value - state.compensation;
        const double next = state.sum + y;
        state.compensation = std::isfinite(next) ? (next - state.sum) - y : 0;
        state.sum = next;
    }

public:
    RollupState state;

    RollupAccumulator(uint64_t interval, Method method) {
        state.interval = interval;
        state.method = static_cast<uint8_t>(method) + 1;
    }

    void add(uint64_t timestamp, const T& value, const RollupState& input = {}) {
        const auto method = state.aggregation();
        if (input.folded() && input.aggregation() != method) {
            throw std::runtime_error("Cannot change the method of already downsampled data");
        }
        if constexpr (std::is_same_v<T, double>) {
            if (!input.folded() && std::isnan(value)) {
                return;
            }
        }
        const uint64_t count = input.folded() ? input.count : 1;
        if (count == 0 || count > UINT64_MAX - state.count) {
            throw std::overflow_error("Invalid or overflowing rollup sample count");
        }
        const uint64_t latest = input.folded() ? input.latestTimestamp : timestamp;
        if (input.folded() && !initialized_) {
            const uint64_t interval = state.interval;
            state = input;
            state.interval = interval;
            value_ = value;
            initialized_ = true;
            return;
        }
        if constexpr (std::is_same_v<T, double>) {
            if (method == Method::SUM || method == Method::AVG) {
                addSum(input.folded() ? input.sum : value);
                // Kahan's compensation is the error to subtract from the next
                // input, not an extra positive contribution.
                if (input.folded() && input.compensation != 0) {
                    addSum(-input.compensation);
                }
            }
        } else if constexpr (std::is_same_v<T, int64_t>) {
            if (method == Method::SUM || method == Method::AVG) {
                const __int128_t contribution = input.folded() ? input.integerSum : static_cast<__int128_t>(value);
                if (__builtin_add_overflow(state.integerSum, contribution, &state.integerSum)) {
                    throw std::overflow_error("Integer rollup accumulator overflow");
                }
            }
        } else if (method != Method::LATEST) {
            throw std::runtime_error("Non-numeric rollups require latest");
        }
        if (!initialized_ || (method == Method::LATEST && latest >= state.latestTimestamp)) {
            value_ = value;
        } else if constexpr (std::is_same_v<T, double> || std::is_same_v<T, int64_t>) {
            if (method == Method::MIN && value < value_)
                value_ = value;
            if (method == Method::MAX && value > value_)
                value_ = value;
        }
        initialized_ = true;
        state.count += count;
        state.latestTimestamp = std::max(state.latestTimestamp, latest);
    }

    T value() const {
        const auto method = state.aggregation();
        if constexpr (std::is_same_v<T, double>) {
            if (method == Method::SUM)
                return state.sum + state.compensation;
            if (method == Method::AVG)
                return (state.sum + state.compensation) / static_cast<double>(state.count);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            if (method == Method::SUM || method == Method::AVG) {
                const __int128_t result = method == Method::AVG ? state.integerSum / state.count : state.integerSum;
                if (result > INT64_MAX || result < INT64_MIN) {
                    throw std::overflow_error("Integer downsample result exceeds int64; source data retained");
                }
                return static_cast<int64_t>(result);
            }
        }
        return value_;
    }
};

// A bounded ascending fold used by compaction and by reads that reconcile
// independently compacted contributions. At equal timestamps, feed the widest
// aggregate first, followed by other aggregates and the newest raw point.
template <typename T>
class RollupStream {
    using Method = timestar::AggregationMethod;
    std::optional<RollupAccumulator<T>> live_;
    uint64_t bucket_ = 0;

public:
    std::vector<uint64_t> timestamps;
    std::vector<T> values;
    std::vector<RollupState> states;

    void finish() {
        if (!live_)
            return;
        if (live_->state.count) {
            timestamps.push_back(bucket_);
            values.push_back(live_->value());
            states.push_back(live_->state);
        }
        live_.reset();
    }

    void add(uint64_t ts, const T& value, const RollupState& state, uint64_t interval = 0,
             Method method = Method::AVG) {
        if (state.folded()) {
            interval = std::max(interval, state.interval);
            // Retention callers validate an explicit method separately. A
            // removed policy still must preserve and merge existing rollups.
            method = state.aggregation();
        }
        if (live_ && ts >= bucket_ && ts - bucket_ < live_->state.interval) {
            if (interval > live_->state.interval)
                throw std::runtime_error("Rollup inputs are not ordered by bucket width");
            live_->add(ts, value, state);
            return;
        }
        finish();
        if (interval == 0) {
            timestamps.push_back(ts);
            values.push_back(value);
            states.push_back(state);
            return;
        }
        bucket_ = (ts / interval) * interval;
        live_.emplace(interval, method);
        live_->add(ts, value, state);
    }
};
