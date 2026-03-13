#include "expression_evaluator.hpp"

#include "anomaly/simd_anomaly.hpp"
#include "forecast/forecast_result.hpp"
#include "transform/transform_functions_simd.hpp"

#include <cmath>
#include <limits>
#include <numeric>

namespace timestar {

// ==================== AlignedSeries Operations ====================

// Minimum array size to benefit from SIMD dispatch overhead.
// Below this, scalar loops (which the compiler may auto-vectorize) are faster.
static constexpr size_t kSimdMinSize = 8;

namespace {
void checkSameSize(const AlignedSeries& a, const AlignedSeries& b, const std::string& operation) {
    if (a.size() != b.size()) {
        throw EvaluationException("Series size mismatch in " + operation + ": " + std::to_string(a.size()) + " vs " +
                                  std::to_string(b.size()));
    }
}
}  // namespace

AlignedSeries AlignedSeries::operator+(const AlignedSeries& other) const {
    checkSameSize(*this, other, "addition");
    // Timestamps are shared via shared_ptr — O(1) pointer copy, no allocation
    std::vector<double> result(values.size());
    if (values.size() >= kSimdMinSize) {
        anomaly::simd::vectorAdd(values.data(), other.values.data(), result.data(), values.size());
    } else {
        for (size_t i = 0; i < values.size(); ++i) {
            result[i] = values[i] + other.values[i];
        }
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::operator-(const AlignedSeries& other) const {
    checkSameSize(*this, other, "subtraction");
    // Timestamps are shared via shared_ptr — O(1) pointer copy, no allocation
    std::vector<double> result(values.size());
    if (values.size() >= kSimdMinSize) {
        anomaly::simd::vectorSubtract(values.data(), other.values.data(), result.data(), values.size());
    } else {
        for (size_t i = 0; i < values.size(); ++i) {
            result[i] = values[i] - other.values[i];
        }
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::operator*(const AlignedSeries& other) const {
    checkSameSize(*this, other, "multiplication");
    // Timestamps are shared via shared_ptr — O(1) pointer copy, no allocation
    std::vector<double> result(values.size());
    if (values.size() >= kSimdMinSize) {
        anomaly::simd::vectorMultiply(values.data(), other.values.data(), result.data(), values.size());
    } else {
        for (size_t i = 0; i < values.size(); ++i) {
            result[i] = values[i] * other.values[i];
        }
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::operator/(const AlignedSeries& other) const {
    checkSameSize(*this, other, "division");
    // Timestamps are shared via shared_ptr — O(1) pointer copy, no allocation
    // IEEE 754 floating-point division naturally produces the correct special values:
    //   x / 0.0 = +/-Inf (sign matches x),  0.0 / 0.0 = NaN
    // This allows the compiler to auto-vectorize the loop without branching.
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = values[i] / other.values[i];
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::operator+(double scalar) const {
    // No vectorScalarAdd in the SIMD library; compiler auto-vectorizes this loop.
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = values[i] + scalar;
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::operator-(double scalar) const {
    // No vectorScalarAdd in the SIMD library; compiler auto-vectorizes this loop.
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = values[i] - scalar;
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::operator*(double scalar) const {
    std::vector<double> result(values.size());
    if (values.size() >= kSimdMinSize) {
        anomaly::simd::vectorScalarMultiply(values.data(), scalar, result.data(), values.size());
    } else {
        for (size_t i = 0; i < values.size(); ++i) {
            result[i] = values[i] * scalar;
        }
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::operator/(double scalar) const {
    if (scalar == 0.0) {
        throw EvaluationException("Division by zero scalar");
    }
    // Multiply by reciprocal — single SIMD broadcast multiply instead of per-element divide.
    std::vector<double> result(values.size());
    const double reciprocal = 1.0 / scalar;
    if (values.size() >= kSimdMinSize) {
        anomaly::simd::vectorScalarMultiply(values.data(), reciprocal, result.data(), values.size());
    } else {
        for (size_t i = 0; i < values.size(); ++i) {
            result[i] = values[i] * reciprocal;
        }
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::negate() const {
    std::vector<double> result(values.size());
    if (values.size() >= kSimdMinSize) {
        anomaly::simd::vectorScalarMultiply(values.data(), -1.0, result.data(), values.size());
    } else {
        for (size_t i = 0; i < values.size(); ++i) {
            result[i] = -values[i];
        }
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::abs() const {
    // transform::simd::abs handles the SIMD threshold internally.
    auto result = transform::simd::abs(values);
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::log() const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = (values[i] > 0) ? std::log(values[i]) : std::numeric_limits<double>::quiet_NaN();
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::log10() const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = (values[i] > 0) ? std::log10(values[i]) : std::numeric_limits<double>::quiet_NaN();
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::sqrt() const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = (values[i] >= 0) ? std::sqrt(values[i]) : std::numeric_limits<double>::quiet_NaN();
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::ceil() const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::ceil(values[i]);
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::floor() const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::floor(values[i]);
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::min(const AlignedSeries& a, const AlignedSeries& b) {
    checkSameSize(a, b, "min");
    std::vector<double> result(a.values.size());
    for (size_t i = 0; i < a.values.size(); ++i) {
        result[i] = std::min(a.values[i], b.values[i]);
    }
    return AlignedSeries(a.timestamps, std::move(result));
}

AlignedSeries AlignedSeries::max(const AlignedSeries& a, const AlignedSeries& b) {
    checkSameSize(a, b, "max");
    std::vector<double> result(a.values.size());
    for (size_t i = 0; i < a.values.size(); ++i) {
        result[i] = std::max(a.values[i], b.values[i]);
    }
    return AlignedSeries(a.timestamps, std::move(result));
}

AlignedSeries AlignedSeries::pow(const AlignedSeries& base, const AlignedSeries& exp) {
    checkSameSize(base, exp, "pow");
    std::vector<double> result(base.values.size());
    for (size_t i = 0; i < base.values.size(); ++i) {
        result[i] = std::pow(base.values[i], exp.values[i]);
    }
    return AlignedSeries(base.timestamps, std::move(result));
}

AlignedSeries AlignedSeries::clamp(const AlignedSeries& val, const AlignedSeries& minVal, const AlignedSeries& maxVal) {
    checkSameSize(val, minVal, "clamp");
    checkSameSize(val, maxVal, "clamp");
    std::vector<double> result(val.values.size());
    for (size_t i = 0; i < val.values.size(); ++i) {
        result[i] = std::clamp(val.values[i], minVal.values[i], maxVal.values[i]);
    }
    return AlignedSeries(val.timestamps, std::move(result));
}

// ==================== Transform Functions ====================

AlignedSeries AlignedSeries::diff() const {
    if (values.empty()) {
        return AlignedSeries(timestamps, {});
    }
    std::vector<double> result(values.size());
    result[0] = std::numeric_limits<double>::quiet_NaN();  // First point has no previous
    for (size_t i = 1; i < values.size(); ++i) {
        result[i] = values[i] - values[i - 1];
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::monotonic_diff() const {
    if (values.empty()) {
        return AlignedSeries(timestamps, {});
    }
    std::vector<double> result(values.size());
    result[0] = std::numeric_limits<double>::quiet_NaN();  // First point has no previous
    for (size_t i = 1; i < values.size(); ++i) {
        double d = values[i] - values[i - 1];
        // Counter reset handling: if diff is negative, assume counter wrapped/reset
        result[i] = (d < 0) ? values[i] : d;
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::default_zero() const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::isnan(values[i]) ? 0.0 : values[i];
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::count_nonzero() const {
    size_t count = 0;
    for (size_t i = 0; i < values.size(); ++i) {
        if (values[i] != 0.0 && !std::isnan(values[i])) {
            ++count;
        }
    }
    // Return a constant series with the count value at all timestamps
    std::vector<double> result(values.size(), static_cast<double>(count));
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::count_not_null() const {
    size_t count = 0;
    for (size_t i = 0; i < values.size(); ++i) {
        if (!std::isnan(values[i])) {
            ++count;
        }
    }
    // Return a constant series with the count value at all timestamps
    std::vector<double> result(values.size(), static_cast<double>(count));
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::rate() const {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    const auto& ts = *timestamps;
    if (values.empty()) {
        return AlignedSeries(timestamps, {});
    }
    std::vector<double> result(values.size());
    result[0] = nan;  // First point has no previous
    for (size_t i = 1; i < values.size(); ++i) {
        double diff = values[i] - values[i - 1];
        if (diff < 0.0)
            diff = 0.0;  // Counter reset: treat as zero increase
        uint64_t delta_ns = (ts[i] > ts[i - 1]) ? (ts[i] - ts[i - 1]) : 0ULL;
        if (delta_ns == 0ULL) {
            result[i] = nan;  // Zero or negative time delta
        } else {
            double delta_seconds = static_cast<double>(delta_ns) / 1e9;
            result[i] = diff / delta_seconds;
        }
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::irate() const {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    const auto& ts = *timestamps;
    if (values.size() < 2) {
        // Fewer than 2 points: return NaN constant series (or empty)
        std::vector<double> result(values.size(), nan);
        return AlignedSeries(timestamps, std::move(result));
    }
    size_t n = values.size();
    double diff = values[n - 1] - values[n - 2];
    double rate_val;
    if (diff < 0.0) {
        // Counter reset at the last step: rate is 0
        rate_val = 0.0;
    } else {
        uint64_t delta_ns = (ts[n - 1] > ts[n - 2]) ? (ts[n - 1] - ts[n - 2]) : 0ULL;
        if (delta_ns == 0ULL) {
            rate_val = nan;
        } else {
            double delta_seconds = static_cast<double>(delta_ns) / 1e9;
            rate_val = diff / delta_seconds;
        }
    }
    // Return the instantaneous rate as a constant series at all timestamps
    std::vector<double> result(values.size(), rate_val);
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::increase() const {
    double total = 0.0;
    for (size_t i = 1; i < values.size(); ++i) {
        double diff = values[i] - values[i - 1];
        if (diff > 0.0) {
            total += diff;
        }
        // Negative diffs (counter resets) contribute 0
    }
    // Return as a constant scalar series
    std::vector<double> result(values.size(), total);
    return AlignedSeries(timestamps, std::move(result));
}

// ==================== Gap-Fill / Interpolation Functions ====================

AlignedSeries AlignedSeries::fill_forward() const {
    std::vector<double> result(values);
    double last = std::numeric_limits<double>::quiet_NaN();
    for (size_t i = 0; i < result.size(); ++i) {
        if (!std::isnan(result[i])) {
            last = result[i];
        } else if (!std::isnan(last)) {
            result[i] = last;
        }
        // else: still NaN (leading run before any real value)
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::fill_backward() const {
    std::vector<double> result(values);
    double next = std::numeric_limits<double>::quiet_NaN();
    for (size_t i = result.size(); i-- > 0;) {
        if (!std::isnan(result[i])) {
            next = result[i];
        } else if (!std::isnan(next)) {
            result[i] = next;
        }
        // else: still NaN (trailing run after the last real value)
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::fill_linear() const {
    std::vector<double> result(values);
    const auto& ts = *timestamps;
    const size_t n = result.size();

    size_t i = 0;
    while (i < n) {
        if (!std::isnan(result[i])) {
            ++i;
            continue;
        }
        // Found the start of a NaN run at index i.
        // Find the end of the run (first non-NaN index after i).
        size_t j = i + 1;
        while (j < n && std::isnan(result[j])) {
            ++j;
        }
        // NaN run is result[i..j-1].
        if (i == 0 || j == n) {
            // Leading or trailing run: leave as NaN.
            i = j;
            continue;
        }
        // Interpolate between result[i-1] (t0, v0) and result[j] (t1, v1).
        double t0 = static_cast<double>(ts[i - 1]);
        double v0 = result[i - 1];
        double t1 = static_cast<double>(ts[j]);
        double v1 = result[j];
        double dt = t1 - t0;
        for (size_t k = i; k < j; ++k) {
            double fraction = (static_cast<double>(ts[k]) - t0) / dt;
            result[k] = v0 + fraction * (v1 - v0);
        }
        i = j;
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::fill_value(double v) const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::isnan(values[i]) ? v : values[i];
    }
    return AlignedSeries(timestamps, std::move(result));
}

// ==================== Accumulation Functions ====================

AlignedSeries AlignedSeries::cumsum() const {
    if (values.empty()) {
        return AlignedSeries(timestamps, {});
    }
    // Build the result values directly rather than copying the whole AlignedSeries
    // and then overwriting every element of the values vector.
    std::vector<double> result(values.size());
    double running = 0.0;
    for (size_t i = 0; i < values.size(); ++i) {
        if (!std::isnan(values[i]))
            running += values[i];
        result[i] = running;
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::integral() const {
    if (values.empty()) {
        return AlignedSeries(timestamps, {});
    }
    const auto& ts = *timestamps;
    // Build the result values directly rather than copying the whole AlignedSeries
    // and then overwriting every element of the values vector.
    std::vector<double> result(values.size());
    double area = 0.0;
    result[0] = 0.0;
    for (size_t i = 1; i < values.size(); ++i) {
        double delta_s = static_cast<double>(ts[i] - ts[i - 1]) / 1e9;
        if (!std::isnan(values[i]) && !std::isnan(values[i - 1]) && delta_s > 0.0) {
            area += (values[i] + values[i - 1]) / 2.0 * delta_s;
        }
        result[i] = area;
    }
    return AlignedSeries(timestamps, std::move(result));
}

// ==================== Normalization ====================

AlignedSeries AlignedSeries::normalize() const {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    std::vector<double> result(values.size(), nan);
    if (values.empty()) {
        return AlignedSeries(timestamps, std::move(result));
    }

    // Find global min and max of all non-NaN values
    double mn = std::numeric_limits<double>::infinity();
    double mx = -std::numeric_limits<double>::infinity();
    for (size_t i = 0; i < values.size(); ++i) {
        if (!std::isnan(values[i])) {
            if (values[i] < mn)
                mn = values[i];
            if (values[i] > mx)
                mx = values[i];
        }
    }

    // If no non-NaN values exist, return all-NaN
    if (mn > mx) {
        return AlignedSeries(timestamps, std::move(result));
    }

    // Constant series (or single point): output 0.0 for non-NaN, NaN for NaN
    if (mx == mn) {
        for (size_t i = 0; i < values.size(); ++i) {
            result[i] = std::isnan(values[i]) ? nan : 0.0;
        }
        return AlignedSeries(timestamps, std::move(result));
    }

    // General case: rescale to [0, 1]
    double range = mx - mn;
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::isnan(values[i]) ? nan : (values[i] - mn) / range;
    }
    return AlignedSeries(timestamps, std::move(result));
}

// ==================== Percent of Total ====================

AlignedSeries AlignedSeries::as_percent(const AlignedSeries& series, const AlignedSeries& total) {
    checkSameSize(series, total, "as_percent");
    const double nan = std::numeric_limits<double>::quiet_NaN();
    std::vector<double> result(series.values.size());
    for (size_t i = 0; i < series.values.size(); ++i) {
        if (std::isnan(series.values[i]) || std::isnan(total.values[i])) {
            result[i] = nan;
        } else if (total.values[i] == 0.0) {
            result[i] = nan;
        } else {
            result[i] = 100.0 * series.values[i] / total.values[i];
        }
    }
    return AlignedSeries(series.timestamps, std::move(result));
}

AlignedSeries AlignedSeries::clamp_min(double minVal) const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::max(values[i], minVal);
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::clamp_max(double maxVal) const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::min(values[i], maxVal);
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::cutoff_min(double threshold) const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = (values[i] < threshold) ? std::numeric_limits<double>::quiet_NaN() : values[i];
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::cutoff_max(double threshold) const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = (values[i] > threshold) ? std::numeric_limits<double>::quiet_NaN() : values[i];
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::rate_per(double seconds_per_point, double scale) const {
    if (values.empty()) {
        return AlignedSeries(timestamps, {});
    }
    const auto& ts = *timestamps;

    std::vector<double> result(values.size());
    result[0] = std::numeric_limits<double>::quiet_NaN();  // First point has no previous

    for (size_t i = 1; i < values.size(); ++i) {
        double value_diff = values[i] - values[i - 1];

        // Compute time delta in seconds from timestamps (nanoseconds)
        if (ts[i] <= ts[i - 1]) {
            result[i] = 0.0;
            continue;
        }
        double dt_seconds = static_cast<double>(ts[i] - ts[i - 1]) / 1e9;

        // Fall back to the provided seconds_per_point if timestamps yield zero
        if (dt_seconds == 0.0) {
            if (seconds_per_point == 0.0) {
                result[i] = 0.0;
                continue;
            }
            dt_seconds = seconds_per_point;
        }

        result[i] = (value_diff / dt_seconds) * scale;
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::per_minute(double seconds_per_point) const {
    return rate_per(seconds_per_point, 60.0);
}

AlignedSeries AlignedSeries::per_hour(double seconds_per_point) const {
    return rate_per(seconds_per_point, 3600.0);
}

// ==================== Rolling Window Functions ====================

AlignedSeries AlignedSeries::rolling_avg(int N) const {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    if (N <= 0) {
        throw EvaluationException("rolling_avg() window size N must be a positive integer");
    }
    std::vector<double> result(values.size(), nan);
    if (values.empty()) {
        return AlignedSeries(timestamps, std::move(result));
    }

    double running_sum = 0.0;
    int valid_count = 0;  // Track non-NaN values in the window
    for (size_t i = 0; i < values.size(); ++i) {
        if (!std::isnan(values[i])) {
            running_sum += values[i];
            valid_count++;
        }
        if (i >= static_cast<size_t>(N)) {
            if (!std::isnan(values[i - N])) {
                running_sum -= values[i - N];
                valid_count--;
            }
        }
        if (i + 1 >= static_cast<size_t>(N)) {
            result[i] = (valid_count > 0) ? running_sum / valid_count : nan;
        }
        // else remains NaN (window not yet full)
    }
    return AlignedSeries(timestamps, std::move(result));
}

// ==================== rolling_monotone (shared ring-buffer impl) ====================
//
// O(N) amortized sliding-window min/max using a contiguous ring buffer instead of
// std::deque.  For window size W the ring holds at most W indices, so for W=100
// the buffer is 101 * 8 = 808 bytes -- fits entirely in L1 cache with zero heap
// indirection (vs. the segment-map overhead of std::deque).
//
// Cmp = std::less<double>{}    -> rolling_min  (ascending invariant, front is min)
// Cmp = std::greater<double>{} -> rolling_max  (descending invariant, front is max)
template <typename Cmp>
static AlignedSeries rolling_monotone(const AlignedSeries& s, int N, Cmp cmp) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    std::vector<double> result(s.values.size(), nan);
    if (s.values.empty()) {
        return AlignedSeries(s.timestamps, std::move(result));
    }

    const size_t wsize = static_cast<size_t>(N);
    // Capacity W+1 so head != tail is always an unambiguous empty test.
    const size_t cap = wsize + 1;
    std::vector<size_t> ring(cap);
    size_t head = 0, tail = 0;

    // All operations are O(1) with no heap traffic.
    auto ring_empty = [&] { return head == tail; };
    auto ring_front = [&] { return ring[head]; };
    auto ring_back = [&] { return ring[(tail + cap - 1) % cap]; };
    auto ring_pop_front = [&] { head = (head + 1) % cap; };
    auto ring_pop_back = [&] { tail = (tail + cap - 1) % cap; };
    auto ring_push_back = [&](size_t v) {
        ring[tail] = v;
        tail = (tail + 1) % cap;
    };

    for (size_t i = 0; i < s.values.size(); ++i) {
        // Evict expired indices from the front.
        while (!ring_empty() && ring_front() + wsize <= i) {
            ring_pop_front();
        }
        // Skip NaN values: they poison comparisons (always false), so once a
        // NaN enters the deque it can never be evicted.  Instead, treat NaN
        // like a missing value -- emit NaN for this position but leave the
        // deque untouched so valid entries remain available.
        if (std::isnan(s.values[i])) {
            // Also evict any NaN entries stuck in the deque from before the
            // NaN-skip logic was in place (defensive).
            while (!ring_empty() && std::isnan(s.values[ring_back()])) {
                ring_pop_back();
            }
            if (i + 1 >= wsize && !ring_empty()) {
                result[i] = s.values[ring_front()];
            }
            // else result[i] is already NaN from initialization
            continue;
        }
        // Maintain the monotone invariant: pop back elements that the current
        // value dominates.  cmp(a, b) is true when a is strictly "more extreme"
        // than b (e.g. a < b for min).  We discard back when NOT more extreme.
        // Also evict NaN entries which can't participate in valid comparisons.
        while (!ring_empty() && (std::isnan(s.values[ring_back()]) || !cmp(s.values[ring_back()], s.values[i]))) {
            ring_pop_back();
        }
        ring_push_back(i);
        if (i + 1 >= wsize) {
            result[i] = s.values[ring_front()];
        }
    }
    return AlignedSeries(s.timestamps, std::move(result));
}

AlignedSeries AlignedSeries::rolling_min(int N) const {
    if (N <= 0) {
        throw EvaluationException("rolling_min() window size N must be a positive integer");
    }
    return rolling_monotone(*this, N, std::less<double>{});
}

AlignedSeries AlignedSeries::rolling_max(int N) const {
    if (N <= 0) {
        throw EvaluationException("rolling_max() window size N must be a positive integer");
    }
    return rolling_monotone(*this, N, std::greater<double>{});
}

AlignedSeries AlignedSeries::rolling_stddev(int N) const {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    if (N <= 0) {
        throw EvaluationException("rolling_stddev() window size N must be a positive integer");
    }
    std::vector<double> result(values.size(), nan);
    if (values.empty()) {
        return AlignedSeries(timestamps, std::move(result));
    }

    // O(N) sliding window using Welford's online algorithm.
    //
    // We maintain a running mean M and a sum of squared deviations from the mean Q
    // (also called the "corrected sum of squares").  Population variance = Q / N.
    //
    // Add rule (element x enters, count grows from k to k+1):
    //   delta = x - M
    //   M    += delta / (k+1)
    //   Q    += delta * (x - M_new)
    //
    // Remove rule (element x leaves, count shrinks from k to k-1, k >= 2):
    //   delta = x - M
    //   M    -= delta / (k-1)
    //   Q    -= delta * (x - M_new)
    //
    // This avoids catastrophic cancellation (S2 - S*S/N) which causes spurious
    // non-zero variance for constant series when the compiler uses FMA instructions.
    //
    // Phase 1 (i = 0..N-2): fill the window, tracking current window size k.
    // Phase 2 (i = N-1..end): window is full; each step adds the new element
    //   then removes the one that fell off the left edge.
    //
    // Pattern C optimisation: in Phase 2 both k (== wsize) and k-1 (== wsize-1)
    // are loop-invariant constants.  We precompute their reciprocals and replace
    // the 13-21 cycle FP divisions with 4 cycle multiplications.

    // Special case: N=1 -- every single-element window has variance 0, stddev 0.
    if (N == 1) {
        for (size_t i = 0; i < values.size(); ++i) {
            result[i] = 0.0;
        }
        return AlignedSeries(timestamps, std::move(result));
    }

    const size_t wsize = static_cast<size_t>(N);
    double M = 0.0;  // current window mean
    double Q = 0.0;  // current sum of squared deviations from M
    size_t k = 0;    // current window occupancy (0 <= k <= N)

    // Phase 1 lambda: k is variable, so we must divide.
    auto welford_add_p1 = [&](double x) {
        ++k;
        double delta = x - M;
        M += delta / static_cast<double>(k);
        Q += delta * (x - M);
    };

    // Phase 1: fill the window (processes elements 0 .. N-2)
    for (size_t i = 0; i < wsize - 1 && i < values.size(); ++i) {
        welford_add_p1(values[i]);
    }

    // Phase 2: k == wsize throughout -- precompute loop-invariant reciprocals.
    const double inv_wsize = 1.0 / static_cast<double>(wsize);
    const double inv_wsize_m1 = (wsize > 1) ? 1.0 / static_cast<double>(wsize - 1) : 0.0;

    for (size_t i = wsize - 1; i < values.size(); ++i) {
        // Add new right-edge element (k goes wsize-1 -> wsize; use inv_wsize).
        ++k;
        {
            double x = values[i];
            double delta = x - M;
            M += delta * inv_wsize;  // replaces: M += delta / k  (k == wsize)
            Q += delta * (x - M);
        }

        result[i] = std::sqrt(Q * inv_wsize);  // replaces: sqrt(Q / wsize)

        if (i + 1 < values.size()) {
            // Remove old left-edge element (k goes wsize -> wsize-1).
            double x = values[i - wsize + 1];
            double delta = x - M;
            M -= delta * inv_wsize_m1;  // replaces: M -= delta / (k-1)  (k-1 == wsize-1)
            Q -= delta * (x - M);
            if (Q < 0.0)
                Q = 0.0;  // clamp rounding noise
            --k;
        }
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::zscore(int N) const {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    if (N <= 0) {
        throw EvaluationException("zscore() window size N must be a positive integer");
    }
    std::vector<double> result(values.size(), nan);
    if (values.empty()) {
        return AlignedSeries(timestamps, std::move(result));
    }

    // Special case: N=1 — single-element window, mean==value, stddev==0, z-score==0.
    // (NaN values in a size-1 window produce NaN output per the original contract.)
    if (N == 1) {
        for (size_t i = 0; i < values.size(); ++i) {
            result[i] = std::isnan(values[i]) ? nan : 0.0;
        }
        return AlignedSeries(timestamps, std::move(result));
    }

    // O(N) sliding window using Welford's online algorithm (same as rolling_stddev).
    // Additionally tracks the count of NaN values in the window: if nan_count > 0,
    // the output is NaN (preserving the original contract).
    //
    // NaN values are treated as 0 in the Welford accumulators so that the
    // add/remove symmetry is maintained, but nan_count forces NaN output.
    // welford_remove requires k >= 2, which is guaranteed for N >= 2 (guarded above).
    //
    // Pattern C optimisation: in Phase 2 both k (== wsize) and k-1 (== wsize-1)
    // are loop-invariant constants.  We precompute their reciprocals.
    const size_t wsize = static_cast<size_t>(N);
    double M = 0.0;  // running window mean (NaN values treated as 0)
    double Q = 0.0;  // running sum of squared deviations from M
    size_t k = 0;    // current window occupancy
    size_t nan_count = 0;

    auto safe = [](double v) { return std::isnan(v) ? 0.0 : v; };

    // Phase 1 lambda: k is variable, so we must divide.
    auto welford_add_p1 = [&](double x) {
        ++k;
        double delta = x - M;
        M += delta / static_cast<double>(k);
        Q += delta * (x - M);
    };

    // Phase 1: fill the window
    for (size_t i = 0; i < wsize - 1 && i < values.size(); ++i) {
        welford_add_p1(safe(values[i]));
        if (std::isnan(values[i]))
            ++nan_count;
    }

    // Phase 2: k == wsize throughout -- precompute loop-invariant reciprocals.
    const double inv_wsize = 1.0 / static_cast<double>(wsize);
    const double inv_wsize_m1 = (wsize > 1) ? 1.0 / static_cast<double>(wsize - 1) : 0.0;

    // Phase 2: slide the window across the rest of the series
    for (size_t i = wsize - 1; i < values.size(); ++i) {
        // Add new right-edge element using Phase-2 reciprocal multiply.
        double v_in = safe(values[i]);
        ++k;
        {
            double delta = v_in - M;
            M += delta * inv_wsize;  // replaces: M += delta / k  (k == wsize)
            Q += delta * (v_in - M);
        }
        if (std::isnan(values[i]))
            ++nan_count;

        if (nan_count > 0) {
            result[i] = nan;
        } else {
            double stddev = std::sqrt(Q * inv_wsize);  // replaces: sqrt(Q / wsize)
            if (stddev == 0.0) {
                result[i] = 0.0;  // constant window: z-score is 0
            } else {
                result[i] = (values[i] - M) * (1.0 / stddev);  // replaces: / stddev
            }
        }

        if (i + 1 < values.size()) {
            size_t remove_idx = i - wsize + 1;
            if (std::isnan(values[remove_idx]))
                --nan_count;
            // Remove old left-edge element using Phase-2 reciprocal multiply.
            double x = safe(values[remove_idx]);
            double delta = x - M;
            M -= delta * inv_wsize_m1;  // replaces: M -= delta / (k-1)  (k-1 == wsize-1)
            Q -= delta * (x - M);
            if (Q < 0.0)
                Q = 0.0;
            --k;
        }
    }
    return AlignedSeries(timestamps, std::move(result));
}

// ==================== Exponential Moving Average ====================

AlignedSeries AlignedSeries::ema(double param) const {
    const double nan = std::numeric_limits<double>::quiet_NaN();

    // Determine alpha: if param > 1, treat as span N => alpha = 2 / (N + 1)
    double alpha = (param > 1.0) ? (2.0 / (param + 1.0)) : param;
    // Clamp alpha to (0, 1]
    if (alpha <= 0.0)
        alpha = std::numeric_limits<double>::min();  // nearly zero but positive
    if (alpha > 1.0)
        alpha = 1.0;

    std::vector<double> result(values.size(), nan);
    if (values.empty()) {
        return AlignedSeries(timestamps, std::move(result));
    }

    // Find the first non-NaN value to seed the EMA
    size_t seed = values.size();  // sentinel: no real value found
    for (size_t i = 0; i < values.size(); ++i) {
        if (!std::isnan(values[i])) {
            seed = i;
            break;
        }
    }

    // If all values are NaN, output is all NaN (already initialized)
    if (seed == values.size()) {
        return AlignedSeries(timestamps, std::move(result));
    }

    // Seed the EMA at the first real value; indices before seed remain NaN
    result[seed] = values[seed];
    double prev_ema = values[seed];

    for (size_t i = seed + 1; i < values.size(); ++i) {
        if (std::isnan(values[i])) {
            // Carry forward the previous EMA value (skip NaN input)
            result[i] = prev_ema;
        } else {
            result[i] = alpha * values[i] + (1.0 - alpha) * prev_ema;
            prev_ema = result[i];
        }
    }

    return AlignedSeries(timestamps, std::move(result));
}

// ==================== Double Exponential Smoothing (Holt-Winters) ====================

AlignedSeries AlignedSeries::holt_winters(double alpha, double beta) const {
    const double nan = std::numeric_limits<double>::quiet_NaN();

    // Clamp alpha and beta to (0, 1]
    if (alpha <= 0.0)
        alpha = std::numeric_limits<double>::min();
    if (alpha > 1.0)
        alpha = 1.0;
    if (beta <= 0.0)
        beta = std::numeric_limits<double>::min();
    if (beta > 1.0)
        beta = 1.0;

    std::vector<double> result(values.size(), nan);
    if (values.empty()) {
        return AlignedSeries(timestamps, std::move(result));
    }

    // Find the first non-NaN value to seed level and trend
    size_t seed = values.size();  // sentinel: no real value found
    for (size_t i = 0; i < values.size(); ++i) {
        if (!std::isnan(values[i])) {
            seed = i;
            break;
        }
    }

    // If all values are NaN, output is all NaN (already initialized)
    if (seed == values.size()) {
        return AlignedSeries(timestamps, std::move(result));
    }

    // Initialize at first real value; trend starts at 0 (single point, no trend info)
    double level = values[seed];
    double trend = 0.0;
    result[seed] = level;

    for (size_t i = seed + 1; i < values.size(); ++i) {
        double prev_level = level;
        if (std::isnan(values[i])) {
            // NaN input: carry forward by projecting level and trend
            level = prev_level + trend;
            // trend unchanged
        } else {
            level = alpha * values[i] + (1.0 - alpha) * (prev_level + trend);
            trend = beta * (level - prev_level) + (1.0 - beta) * trend;
        }
        result[i] = level;
    }

    return AlignedSeries(timestamps, std::move(result));
}

// ==================== Timestamp Shift ====================

AlignedSeries AlignedSeries::time_shift(int64_t offsetNs) const {
    // Shift every timestamp by offsetNs nanoseconds; values are unchanged.
    // Must create a new shared_ptr because timestamps change.
    const auto& ts = *timestamps;
    auto shifted = std::make_shared<std::vector<uint64_t>>(ts.size());
    for (size_t i = 0; i < ts.size(); ++i) {
        // Cast to int64_t to allow negative offsets, clamp to 0 to avoid wrap.
        int64_t result = static_cast<int64_t>(ts[i]) + offsetNs;
        (*shifted)[i] = (result < 0) ? 0 : static_cast<uint64_t>(result);
    }
    return AlignedSeries(std::move(shifted), values);
}

// ==================== ExpressionEvaluator ====================

AlignedSeries ExpressionEvaluator::evaluate(const ExpressionNode& expr, const QueryResultMap& queryResults) {
    if (queryResults.empty()) {
        throw EvaluationException("No query results provided for evaluation");
    }

    return evaluateNode(expr, queryResults);
}

AlignedSeries ExpressionEvaluator::evaluateNode(const ExpressionNode& node, const QueryResultMap& queryResults) {
    switch (node.type) {
        case ExprNodeType::QUERY_REF:
            return evaluateQueryRef(node.asQueryRef(), queryResults);

        case ExprNodeType::SCALAR:
            return evaluateScalar(node.asScalar(), queryResults);

        case ExprNodeType::BINARY_OP:
            return evaluateBinaryOp(node.asBinaryOp(), queryResults);

        case ExprNodeType::UNARY_OP:
            return evaluateUnaryOp(node.asUnaryOp(), queryResults);

        case ExprNodeType::FUNCTION_CALL:
            return evaluateFunctionCall(node.asFunctionCall(), queryResults);

        case ExprNodeType::TIME_SHIFT_FUNCTION:
            return evaluateTimeShiftFunction(node.asTimeShiftFunction(), queryResults);

        case ExprNodeType::ANOMALY_FUNCTION:
            throw EvaluationException("anomalies() is not yet implemented");

        case ExprNodeType::FORECAST_FUNCTION:
            throw EvaluationException("forecast() is not yet implemented");

        default:
            throw EvaluationException("Unknown expression node type");
    }
}

const AlignedSeries& ExpressionEvaluator::evaluateQueryRef(const QueryRef& ref, const QueryResultMap& queryResults) {
    auto it = queryResults.find(ref.name);
    if (it == queryResults.end()) {
        throw EvaluationException("Query '" + ref.name + "' not found in results");
    }
    return it->second;
}

AlignedSeries ExpressionEvaluator::evaluateScalar(const ScalarValue& scalar, const QueryResultMap& queryResults) {
    auto tsPtr = getReferenceTimestamps(queryResults);
    return makeScalarSeries(scalar.value, tsPtr);
}

AlignedSeries ExpressionEvaluator::evaluateBinaryOp(const BinaryOp& op, const QueryResultMap& queryResults) {
    auto left = evaluateNode(*op.left, queryResults);
    auto right = evaluateNode(*op.right, queryResults);

    switch (op.op) {
        case BinaryOpType::ADD:
            return left + right;
        case BinaryOpType::SUBTRACT:
            return left - right;
        case BinaryOpType::MULTIPLY:
            return left * right;
        case BinaryOpType::DIVIDE:
            return left / right;
        default:
            throw EvaluationException("Unknown binary operator");
    }
}

AlignedSeries ExpressionEvaluator::evaluateUnaryOp(const UnaryOp& op, const QueryResultMap& queryResults) {
    auto operand = evaluateNode(*op.operand, queryResults);

    switch (op.op) {
        case UnaryOpType::NEGATE:
            return operand.negate();
        case UnaryOpType::ABS:
            return operand.abs();
        case UnaryOpType::LOG:
            return operand.log();
        case UnaryOpType::LOG10:
            return operand.log10();
        case UnaryOpType::SQRT:
            return operand.sqrt();
        case UnaryOpType::CEIL:
            return operand.ceil();
        case UnaryOpType::FLOOR:
            return operand.floor();
        // Transform functions
        case UnaryOpType::DIFF:
            return operand.diff();
        case UnaryOpType::MONOTONIC_DIFF:
            return operand.monotonic_diff();
        case UnaryOpType::DEFAULT_ZERO:
            return operand.default_zero();
        case UnaryOpType::COUNT_NONZERO:
            return operand.count_nonzero();
        case UnaryOpType::COUNT_NOT_NULL:
            return operand.count_not_null();
        // Counter-rate functions
        case UnaryOpType::RATE:
            return operand.rate();
        case UnaryOpType::IRATE:
            return operand.irate();
        case UnaryOpType::INCREASE:
            return operand.increase();
        // Gap-fill / interpolation functions
        case UnaryOpType::FILL_FORWARD:
            return operand.fill_forward();
        case UnaryOpType::FILL_BACKWARD:
            return operand.fill_backward();
        case UnaryOpType::FILL_LINEAR:
            return operand.fill_linear();
        // Accumulation functions
        case UnaryOpType::CUMSUM:
            return operand.cumsum();
        case UnaryOpType::INTEGRAL:
            return operand.integral();
        // Normalization
        case UnaryOpType::NORMALIZE:
            return operand.normalize();
        default:
            throw EvaluationException("Unknown unary operator");
    }
}

AlignedSeries ExpressionEvaluator::evaluateFunctionCall(const FunctionCall& call, const QueryResultMap& queryResults) {
    switch (call.func) {
        case FunctionType::MIN: {
            if (call.args.size() != 2) {
                throw EvaluationException("min() requires exactly 2 arguments");
            }
            auto a = evaluateNode(*call.args[0], queryResults);
            auto b = evaluateNode(*call.args[1], queryResults);
            return AlignedSeries::min(a, b);
        }

        case FunctionType::MAX: {
            if (call.args.size() != 2) {
                throw EvaluationException("max() requires exactly 2 arguments");
            }
            auto a = evaluateNode(*call.args[0], queryResults);
            auto b = evaluateNode(*call.args[1], queryResults);
            return AlignedSeries::max(a, b);
        }

        case FunctionType::POW: {
            if (call.args.size() != 2) {
                throw EvaluationException("pow() requires exactly 2 arguments");
            }
            auto base = evaluateNode(*call.args[0], queryResults);
            auto exp = evaluateNode(*call.args[1], queryResults);
            return AlignedSeries::pow(base, exp);
        }

        case FunctionType::CLAMP: {
            if (call.args.size() != 3) {
                throw EvaluationException("clamp() requires exactly 3 arguments");
            }
            auto val = evaluateNode(*call.args[0], queryResults);
            auto minVal = evaluateNode(*call.args[1], queryResults);
            auto maxVal = evaluateNode(*call.args[2], queryResults);
            return AlignedSeries::clamp(val, minVal, maxVal);
        }

        // Transform functions with scalar argument
        // Note: The second argument is expected to be a scalar (constant series).
        // We extract only values[0] from it; a non-scalar series will use its first value.
        case FunctionType::CLAMP_MIN: {
            if (call.args.size() != 2) {
                throw EvaluationException("clamp_min() requires exactly 2 arguments");
            }
            auto series = evaluateNode(*call.args[0], queryResults);
            auto scalarSeries = evaluateNode(*call.args[1], queryResults);
            if (scalarSeries.empty()) {
                throw EvaluationException("clamp_min() second argument must be a non-empty scalar");
            }
            return series.clamp_min(scalarSeries.values[0]);
        }

        case FunctionType::CLAMP_MAX: {
            if (call.args.size() != 2) {
                throw EvaluationException("clamp_max() requires exactly 2 arguments");
            }
            auto series = evaluateNode(*call.args[0], queryResults);
            auto scalarSeries = evaluateNode(*call.args[1], queryResults);
            if (scalarSeries.empty()) {
                throw EvaluationException("clamp_max() second argument must be a non-empty scalar");
            }
            return series.clamp_max(scalarSeries.values[0]);
        }

        case FunctionType::CUTOFF_MIN: {
            if (call.args.size() != 2) {
                throw EvaluationException("cutoff_min() requires exactly 2 arguments");
            }
            auto series = evaluateNode(*call.args[0], queryResults);
            auto scalarSeries = evaluateNode(*call.args[1], queryResults);
            if (scalarSeries.empty()) {
                throw EvaluationException("cutoff_min() second argument must be a non-empty scalar");
            }
            return series.cutoff_min(scalarSeries.values[0]);
        }

        case FunctionType::CUTOFF_MAX: {
            if (call.args.size() != 2) {
                throw EvaluationException("cutoff_max() requires exactly 2 arguments");
            }
            auto series = evaluateNode(*call.args[0], queryResults);
            auto scalarSeries = evaluateNode(*call.args[1], queryResults);
            if (scalarSeries.empty()) {
                throw EvaluationException("cutoff_max() second argument must be a non-empty scalar");
            }
            return series.cutoff_max(scalarSeries.values[0]);
        }

        case FunctionType::PER_MINUTE: {
            if (call.args.size() != 2) {
                throw EvaluationException("per_minute() requires exactly 2 arguments");
            }
            auto series = evaluateNode(*call.args[0], queryResults);
            auto scalarSeries = evaluateNode(*call.args[1], queryResults);
            if (scalarSeries.empty()) {
                throw EvaluationException("per_minute() second argument must be a non-empty scalar");
            }
            return series.per_minute(scalarSeries.values[0]);
        }

        case FunctionType::PER_HOUR: {
            if (call.args.size() != 2) {
                throw EvaluationException("per_hour() requires exactly 2 arguments");
            }
            auto series = evaluateNode(*call.args[0], queryResults);
            auto scalarSeries = evaluateNode(*call.args[1], queryResults);
            if (scalarSeries.empty()) {
                throw EvaluationException("per_hour() second argument must be a non-empty scalar");
            }
            return series.per_hour(scalarSeries.values[0]);
        }

        // Rolling window functions: second argument is integer window size N
        case FunctionType::ROLLING_AVG: {
            if (call.args.size() != 2) {
                throw EvaluationException("rolling_avg() requires exactly 2 arguments");
            }
            auto series = evaluateNode(*call.args[0], queryResults);
            auto scalarSeries = evaluateNode(*call.args[1], queryResults);
            if (scalarSeries.empty()) {
                throw EvaluationException("rolling_avg() second argument must be a non-empty scalar");
            }
            int N = static_cast<int>(scalarSeries.values[0]);
            if (N <= 0) {
                throw EvaluationException("rolling_avg() window size N must be a positive integer");
            }
            return series.rolling_avg(N);
        }

        case FunctionType::ROLLING_MIN: {
            if (call.args.size() != 2) {
                throw EvaluationException("rolling_min() requires exactly 2 arguments");
            }
            auto series = evaluateNode(*call.args[0], queryResults);
            auto scalarSeries = evaluateNode(*call.args[1], queryResults);
            if (scalarSeries.empty()) {
                throw EvaluationException("rolling_min() second argument must be a non-empty scalar");
            }
            int N = static_cast<int>(scalarSeries.values[0]);
            if (N <= 0) {
                throw EvaluationException("rolling_min() window size N must be a positive integer");
            }
            return series.rolling_min(N);
        }

        case FunctionType::ROLLING_MAX: {
            if (call.args.size() != 2) {
                throw EvaluationException("rolling_max() requires exactly 2 arguments");
            }
            auto series = evaluateNode(*call.args[0], queryResults);
            auto scalarSeries = evaluateNode(*call.args[1], queryResults);
            if (scalarSeries.empty()) {
                throw EvaluationException("rolling_max() second argument must be a non-empty scalar");
            }
            int N = static_cast<int>(scalarSeries.values[0]);
            if (N <= 0) {
                throw EvaluationException("rolling_max() window size N must be a positive integer");
            }
            return series.rolling_max(N);
        }

        case FunctionType::ROLLING_STDDEV: {
            if (call.args.size() != 2) {
                throw EvaluationException("rolling_stddev() requires exactly 2 arguments");
            }
            auto series = evaluateNode(*call.args[0], queryResults);
            auto scalarSeries = evaluateNode(*call.args[1], queryResults);
            if (scalarSeries.empty()) {
                throw EvaluationException("rolling_stddev() second argument must be a non-empty scalar");
            }
            int N = static_cast<int>(scalarSeries.values[0]);
            if (N <= 0) {
                throw EvaluationException("rolling_stddev() window size N must be a positive integer");
            }
            return series.rolling_stddev(N);
        }

        case FunctionType::FILL_VALUE: {
            if (call.args.size() != 2) {
                throw EvaluationException("fill_value() requires exactly 2 arguments");
            }
            auto series = evaluateNode(*call.args[0], queryResults);
            auto scalarSeries = evaluateNode(*call.args[1], queryResults);
            if (scalarSeries.empty()) {
                throw EvaluationException("fill_value() second argument must be a non-empty scalar");
            }
            return series.fill_value(scalarSeries.values[0]);
        }

        case FunctionType::EMA: {
            if (call.args.size() != 2) {
                throw EvaluationException("ema() requires exactly 2 arguments");
            }
            auto series = evaluateNode(*call.args[0], queryResults);
            auto scalarSeries = evaluateNode(*call.args[1], queryResults);
            if (scalarSeries.empty()) {
                throw EvaluationException("ema() second argument must be a non-empty scalar");
            }
            double param = scalarSeries.values[0];
            if (param <= 0.0) {
                throw EvaluationException("ema() parameter must be positive (alpha in (0,1] or span > 1)");
            }
            return series.ema(param);
        }

        case FunctionType::HOLT_WINTERS: {
            if (call.args.size() != 3) {
                throw EvaluationException("holt_winters() requires exactly 3 arguments");
            }
            auto series = evaluateNode(*call.args[0], queryResults);
            auto alphaSeries = evaluateNode(*call.args[1], queryResults);
            auto betaSeries = evaluateNode(*call.args[2], queryResults);
            if (alphaSeries.empty()) {
                throw EvaluationException("holt_winters() alpha (second argument) must be a non-empty scalar");
            }
            if (betaSeries.empty()) {
                throw EvaluationException("holt_winters() beta (third argument) must be a non-empty scalar");
            }
            double alpha = alphaSeries.values[0];
            double beta = betaSeries.values[0];
            if (alpha <= 0.0 || alpha > 1.0) {
                throw EvaluationException("holt_winters() alpha must be in (0, 1]");
            }
            if (beta <= 0.0 || beta > 1.0) {
                throw EvaluationException("holt_winters() beta must be in (0, 1]");
            }
            return series.holt_winters(alpha, beta);
        }

        case FunctionType::ZSCORE: {
            if (call.args.size() != 2) {
                throw EvaluationException("zscore() requires exactly 2 arguments");
            }
            auto series = evaluateNode(*call.args[0], queryResults);
            auto scalarSeries = evaluateNode(*call.args[1], queryResults);
            if (scalarSeries.empty()) {
                throw EvaluationException("zscore() second argument must be a non-empty scalar");
            }
            int N = static_cast<int>(scalarSeries.values[0]);
            if (N <= 0) {
                throw EvaluationException("zscore() window size N must be a positive integer");
            }
            return series.zscore(N);
        }

        case FunctionType::AS_PERCENT: {
            if (call.args.size() != 2) {
                throw EvaluationException("as_percent() requires exactly 2 arguments");
            }
            auto series = evaluateNode(*call.args[0], queryResults);
            auto total = evaluateNode(*call.args[1], queryResults);
            return AlignedSeries::as_percent(series, total);
        }

        // topk/bottomk in single-series context: N scalar, then series reference.
        // With only one group in the QueryResultMap, topk(N, series) where N >= 1
        // always returns that series unchanged (there is only one group to keep).
        // When N < 1, return an empty series (all groups filtered out).
        case FunctionType::TOPK:
        case FunctionType::BOTTOMK: {
            if (call.args.size() != 2) {
                const char* fname = (call.func == FunctionType::TOPK) ? "topk" : "bottomk";
                throw EvaluationException(std::string(fname) + "() requires exactly 2 arguments");
            }
            auto scalarSeries = evaluateNode(*call.args[0], queryResults);
            if (scalarSeries.empty()) {
                const char* fname = (call.func == FunctionType::TOPK) ? "topk" : "bottomk";
                throw EvaluationException(std::string(fname) + "() first argument (N) must be a non-empty scalar");
            }
            int N = static_cast<int>(scalarSeries.values[0]);
            if (N < 0) {
                const char* fname = (call.func == FunctionType::TOPK) ? "topk" : "bottomk";
                throw EvaluationException(std::string(fname) + "() N must be non-negative");
            }
            auto series = evaluateNode(*call.args[1], queryResults);
            // In single-series context there is exactly one "group".
            // If N >= 1, the group is kept; if N == 0, return an empty series.
            if (N == 0) {
                return AlignedSeries(std::vector<uint64_t>{}, std::vector<double>{});
            }
            return series;  // N >= 1: single group is always kept
        }

            // ---- Cross-series aggregation (variadic, element-wise) ----
            //
            // All input series must already be aligned (same timestamps).
            // Each function accepts >= 1 series argument (avg/sum/min/max)
            // or >= 2 arguments where the first is a scalar p (percentile_of_series).
            //
            // For each index i, gather the non-NaN values from all series and apply
            // the aggregation. If no non-NaN value exists at index i, output NaN.

        case FunctionType::AVG_OF_SERIES: {
            if (call.args.empty()) {
                throw EvaluationException("avg_of_series() requires at least 1 argument");
            }
            // Evaluate all series arguments
            std::vector<AlignedSeries> series;
            series.reserve(call.args.size());
            for (const auto& arg : call.args) {
                series.push_back(evaluateNode(*arg, queryResults));
            }
            // Verify sizes match
            size_t n = series[0].size();
            for (size_t i = 1; i < series.size(); ++i) {
                if (series[i].size() != n) {
                    throw EvaluationException("avg_of_series(): all series must have the same length");
                }
            }
            const double nan = std::numeric_limits<double>::quiet_NaN();
            std::vector<double> result(n, nan);
            for (size_t i = 0; i < n; ++i) {
                double sum = 0.0;
                size_t cnt = 0;
                for (const auto& s : series) {
                    if (!std::isnan(s.values[i])) {
                        sum += s.values[i];
                        ++cnt;
                    }
                }
                if (cnt > 0)
                    result[i] = sum / static_cast<double>(cnt);
            }
            return AlignedSeries(series[0].timestamps, std::move(result));
        }

        case FunctionType::SUM_OF_SERIES: {
            if (call.args.empty()) {
                throw EvaluationException("sum_of_series() requires at least 1 argument");
            }
            std::vector<AlignedSeries> series;
            series.reserve(call.args.size());
            for (const auto& arg : call.args) {
                series.push_back(evaluateNode(*arg, queryResults));
            }
            size_t n = series[0].size();
            for (size_t i = 1; i < series.size(); ++i) {
                if (series[i].size() != n) {
                    throw EvaluationException("sum_of_series(): all series must have the same length");
                }
            }
            const double nan = std::numeric_limits<double>::quiet_NaN();
            std::vector<double> result(n, nan);
            for (size_t i = 0; i < n; ++i) {
                double sum = 0.0;
                size_t cnt = 0;
                for (const auto& s : series) {
                    if (!std::isnan(s.values[i])) {
                        sum += s.values[i];
                        ++cnt;
                    }
                }
                if (cnt > 0)
                    result[i] = sum;
            }
            return AlignedSeries(series[0].timestamps, std::move(result));
        }

        case FunctionType::MIN_OF_SERIES: {
            if (call.args.empty()) {
                throw EvaluationException("min_of_series() requires at least 1 argument");
            }
            std::vector<AlignedSeries> series;
            series.reserve(call.args.size());
            for (const auto& arg : call.args) {
                series.push_back(evaluateNode(*arg, queryResults));
            }
            size_t n = series[0].size();
            for (size_t i = 1; i < series.size(); ++i) {
                if (series[i].size() != n) {
                    throw EvaluationException("min_of_series(): all series must have the same length");
                }
            }
            const double nan = std::numeric_limits<double>::quiet_NaN();
            const double inf = std::numeric_limits<double>::infinity();
            std::vector<double> result(n, nan);
            for (size_t i = 0; i < n; ++i) {
                double minVal = inf;
                bool found = false;
                for (const auto& s : series) {
                    if (!std::isnan(s.values[i])) {
                        if (!found || s.values[i] < minVal) {
                            minVal = s.values[i];
                            found = true;
                        }
                    }
                }
                if (found)
                    result[i] = minVal;
            }
            return AlignedSeries(series[0].timestamps, std::move(result));
        }

        case FunctionType::MAX_OF_SERIES: {
            if (call.args.empty()) {
                throw EvaluationException("max_of_series() requires at least 1 argument");
            }
            std::vector<AlignedSeries> series;
            series.reserve(call.args.size());
            for (const auto& arg : call.args) {
                series.push_back(evaluateNode(*arg, queryResults));
            }
            size_t n = series[0].size();
            for (size_t i = 1; i < series.size(); ++i) {
                if (series[i].size() != n) {
                    throw EvaluationException("max_of_series(): all series must have the same length");
                }
            }
            const double nan = std::numeric_limits<double>::quiet_NaN();
            const double ninf = -std::numeric_limits<double>::infinity();
            std::vector<double> result(n, nan);
            for (size_t i = 0; i < n; ++i) {
                double maxVal = ninf;
                bool found = false;
                for (const auto& s : series) {
                    if (!std::isnan(s.values[i])) {
                        if (!found || s.values[i] > maxVal) {
                            maxVal = s.values[i];
                            found = true;
                        }
                    }
                }
                if (found)
                    result[i] = maxVal;
            }
            return AlignedSeries(series[0].timestamps, std::move(result));
        }

        case FunctionType::PERCENTILE_OF_SERIES: {
            // Syntax: percentile_of_series(p, series_a, series_b, ...)
            // First argument is the percentile p (scalar), remaining are series.
            if (call.args.size() < 2) {
                throw EvaluationException(
                    "percentile_of_series() requires at least 2 arguments "
                    "(p, series...)");
            }
            // Evaluate p (first arg, expected to be a scalar or scalar series)
            auto pSeries = evaluateNode(*call.args[0], queryResults);
            if (pSeries.empty()) {
                throw EvaluationException("percentile_of_series() first argument (p) must be a non-empty scalar");
            }
            double p = pSeries.values[0];
            if (p < 0.0 || p > 100.0) {
                throw EvaluationException("percentile_of_series() p must be in [0, 100], got " + std::to_string(p));
            }

            // Evaluate the series arguments
            std::vector<AlignedSeries> series;
            series.reserve(call.args.size() - 1);
            for (size_t a = 1; a < call.args.size(); ++a) {
                series.push_back(evaluateNode(*call.args[a], queryResults));
            }
            size_t n = series[0].size();
            for (size_t i = 1; i < series.size(); ++i) {
                if (series[i].size() != n) {
                    throw EvaluationException("percentile_of_series(): all series must have the same length");
                }
            }

            const double nan = std::numeric_limits<double>::quiet_NaN();
            std::vector<double> result(n, nan);
            std::vector<double> tmp;
            tmp.reserve(series.size());

            for (size_t i = 0; i < n; ++i) {
                tmp.clear();
                for (const auto& s : series) {
                    if (!std::isnan(s.values[i])) {
                        tmp.push_back(s.values[i]);
                    }
                }
                if (tmp.empty())
                    continue;  // result[i] stays NaN

                std::sort(tmp.begin(), tmp.end());
                // Linear interpolation percentile
                double idx = (p / 100.0) * static_cast<double>(tmp.size() - 1);
                size_t lo = static_cast<size_t>(std::floor(idx));
                size_t hi = lo + 1;
                if (hi >= tmp.size()) {
                    result[i] = tmp.back();
                } else {
                    double frac = idx - static_cast<double>(lo);
                    result[i] = tmp[lo] * (1.0 - frac) + tmp[hi] * frac;
                }
            }
            return AlignedSeries(series[0].timestamps, std::move(result));
        }

        default:
            throw EvaluationException("Unknown function type");
    }
}

AlignedSeries ExpressionEvaluator::makeScalarSeries(double value,
                                                    const std::shared_ptr<const std::vector<uint64_t>>& tsPtr) const {
    std::vector<double> values(tsPtr->size(), value);
    return AlignedSeries(tsPtr, std::move(values));
}

std::shared_ptr<const std::vector<uint64_t>> ExpressionEvaluator::getReferenceTimestamps(
    const QueryResultMap& queryResults) const {
    // Use the first non-empty series timestamps as reference
    for (const auto& [name, series] : queryResults) {
        if (!series.empty()) {
            return series.timestamps;
        }
    }
    throw EvaluationException("No non-empty series found in query results");
}

AlignedSeries ExpressionEvaluator::evaluateTimeShiftFunction(const TimeShiftFunction& ts,
                                                             const QueryResultMap& queryResults) {
    // Retrieve the series for the query reference
    auto it = queryResults.find(ts.queryRef);
    if (it == queryResults.end()) {
        throw EvaluationException("time_shift(): query '" + ts.queryRef + "' not found in results");
    }
    const AlignedSeries& series = it->second;

    // Parse the offset string.
    // A leading '-' means negative offset; strip it and negate the result.
    const std::string& offsetStr = ts.offset;
    bool negative = (!offsetStr.empty() && offsetStr[0] == '-');
    std::string absStr = negative ? offsetStr.substr(1) : offsetStr;

    if (absStr.empty()) {
        throw EvaluationException("time_shift(): offset string is empty or just '-'");
    }

    uint64_t absNs;
    try {
        absNs = forecast::parseDurationToNs(absStr);
    } catch (const std::exception& e) {
        throw EvaluationException(std::string("time_shift(): invalid offset '") + offsetStr + "': " + e.what());
    }

    int64_t offsetNs = negative ? -static_cast<int64_t>(absNs) : static_cast<int64_t>(absNs);

    return series.time_shift(offsetNs);
}

}  // namespace timestar
