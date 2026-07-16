#include "expression_evaluator.hpp"

#include "anomaly/simd_anomaly.hpp"
#include "forecast/forecast_result.hpp"
#include "simd_aggregator.hpp"
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

AlignedSeries AlignedSeries::rsub(double scalar) const {
    // scalar - values[i]; compiler auto-vectorizes this loop.
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = scalar - values[i];
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::rdiv(double scalar) const {
    // scalar / values[i]; IEEE semantics (x/0 -> +/-Inf, 0/0 -> NaN) match the
    // series-series division path, so this is a drop-in for scalar literals.
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = scalar / values[i];
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::divIeee(double scalar) const {
    // values[i] / scalar with true division (bit-identical to the series-series
    // path against a constant series). Unlike operator/(double), does not throw
    // on scalar == 0 and does not use the reciprocal-multiply shortcut.
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = values[i] / scalar;
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

AlignedSeries AlignedSeries::exp() const {
    return AlignedSeries(timestamps, transform::simd::exp(values));
}

AlignedSeries AlignedSeries::roundNearest() const {
    return AlignedSeries(timestamps, transform::simd::round(values));
}

AlignedSeries AlignedSeries::sign() const {
    return AlignedSeries(timestamps, transform::simd::sign(values));
}

AlignedSeries AlignedSeries::min(const AlignedSeries& a, const AlignedSeries& b) {
    checkSameSize(a, b, "min");
    std::vector<double> result(a.values.size());
    for (size_t i = 0; i < a.values.size(); ++i) {
        result[i] = std::fmin(a.values[i], b.values[i]);  // NaN-safe: returns non-NaN arg
    }
    return AlignedSeries(a.timestamps, std::move(result));
}

AlignedSeries AlignedSeries::max(const AlignedSeries& a, const AlignedSeries& b) {
    checkSameSize(a, b, "max");
    std::vector<double> result(a.values.size());
    for (size_t i = 0; i < a.values.size(); ++i) {
        result[i] = std::fmax(a.values[i], b.values[i]);  // NaN-safe: returns non-NaN arg
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
        double lo = minVal.values[i], hi = maxVal.values[i];
        double v = val.values[i];
        if (std::isnan(v) || std::isnan(lo) || std::isnan(hi) || lo > hi) {
            result[i] = std::numeric_limits<double>::quiet_NaN();
        } else {
            result[i] = std::clamp(v, lo, hi);
        }
    }
    return AlignedSeries(val.timestamps, std::move(result));
}

// ==================== Transform Functions ====================

AlignedSeries AlignedSeries::diff() const {
    if (values.empty()) {
        return AlignedSeries(timestamps, {});
    }
    // NaN-fill construction: element 0 keeps NaN (first point has no previous);
    // the loop overwrites every later element. Also avoids a GCC false-positive
    // -Wnull-dereference on an unconditional result[0] store.
    std::vector<double> result(values.size(), std::numeric_limits<double>::quiet_NaN());
    for (size_t i = 1; i < values.size(); ++i) {
        result[i] = values[i] - values[i - 1];
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::monotonicDiff() const {
    if (values.empty()) {
        return AlignedSeries(timestamps, {});
    }
    // NaN-fill construction: element 0 keeps NaN (first point has no previous);
    // the loop overwrites every later element.
    std::vector<double> result(values.size(), std::numeric_limits<double>::quiet_NaN());
    for (size_t i = 1; i < values.size(); ++i) {
        double d = values[i] - values[i - 1];
        // Counter reset handling: if diff is negative, assume counter wrapped/reset
        result[i] = (d < 0) ? values[i] : d;
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::defaultZero() const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::isnan(values[i]) ? 0.0 : values[i];
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::countNonzero() const {
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

AlignedSeries AlignedSeries::countNotNull() const {
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
    // NaN-fill construction: element 0 keeps NaN (first point has no previous);
    // the loop overwrites every later element.
    std::vector<double> result(values.size(), nan);
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

// ==================== Gauge Derivative / Range Summaries ====================

AlignedSeries AlignedSeries::deriv() const {
    if (values.empty()) {
        return AlignedSeries(timestamps, {});
    }
    return AlignedSeries(timestamps, transform::simd::deriv(values, *timestamps));
}

AlignedSeries AlignedSeries::delta() const {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    double first = nan, last = nan;
    for (size_t i = 0; i < values.size(); ++i) {
        if (!std::isnan(values[i])) {
            first = values[i];
            break;
        }
    }
    for (size_t i = values.size(); i-- > 0;) {
        if (!std::isnan(values[i])) {
            last = values[i];
            break;
        }
    }
    const double d = (std::isnan(first)) ? nan : last - first;
    std::vector<double> result(values.size(), d);
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::idelta() const {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    double last = nan, prev = nan;
    for (size_t i = values.size(); i-- > 0;) {
        if (!std::isnan(values[i])) {
            if (std::isnan(last)) {
                last = values[i];
            } else {
                prev = values[i];
                break;
            }
        }
    }
    const double d = (std::isnan(last) || std::isnan(prev)) ? nan : last - prev;
    std::vector<double> result(values.size(), d);
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::changes() const {
    // Count of value changes between consecutive non-NaN observations
    // (NaN gaps are skipped, not counted as changes).
    size_t count = 0;
    double prev = std::numeric_limits<double>::quiet_NaN();
    for (size_t i = 0; i < values.size(); ++i) {
        if (std::isnan(values[i])) {
            continue;
        }
        if (!std::isnan(prev) && values[i] != prev) {
            ++count;
        }
        prev = values[i];
    }
    std::vector<double> result(values.size(), static_cast<double>(count));
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::resets() const {
    // Count of decreases between consecutive non-NaN observations
    // (counter resets for monotonically-increasing counters).
    size_t count = 0;
    double prev = std::numeric_limits<double>::quiet_NaN();
    for (size_t i = 0; i < values.size(); ++i) {
        if (std::isnan(values[i])) {
            continue;
        }
        if (!std::isnan(prev) && values[i] < prev) {
            ++count;
        }
        prev = values[i];
    }
    std::vector<double> result(values.size(), static_cast<double>(count));
    return AlignedSeries(timestamps, std::move(result));
}

// ==================== Gap-Fill / Interpolation Functions ====================

AlignedSeries AlignedSeries::fillForward() const {
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

AlignedSeries AlignedSeries::fillBackward() const {
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

AlignedSeries AlignedSeries::fillLinear() const {
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
        if (dt == 0.0) {
            // Duplicate timestamps — use midpoint of endpoint values
            double mid = (v0 + v1) * 0.5;
            for (size_t k = i; k < j; ++k) {
                result[k] = mid;
            }
        } else {
            for (size_t k = i; k < j; ++k) {
                double fraction = (static_cast<double>(ts[k]) - t0) / dt;
                result[k] = v0 + fraction * (v1 - v0);
            }
        }
        i = j;
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::fillSpline() const {
    // Natural cubic spline interpolation through the known (non-NaN) points.
    // Semantics match fill_linear: only interior NaN runs are filled;
    // leading/trailing NaN runs stay NaN.  Fewer than 3 knots degenerates to
    // linear interpolation (a spline needs >= 3 points to bend).
    const size_t n = values.size();
    const auto& ts = *timestamps;

    // Collect knots (indices of finite values).
    std::vector<size_t> knotIdx;
    knotIdx.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        if (!std::isnan(values[i])) {
            knotIdx.push_back(i);
        }
    }
    const size_t k = knotIdx.size();
    if (k < 3 || k == n) {
        // Not enough knots for a cubic, or nothing to fill.
        return k == n ? *this : fillLinear();
    }

    // Knot coordinates.  Timestamps are nanoseconds (~1e18), beyond double's
    // 2^53 integer range — normalize to milliseconds relative to the first
    // knot so intervals stay exactly representable.
    const uint64_t tBase = ts[knotIdx[0]];
    std::vector<double> x(k), yv(k);
    for (size_t i = 0; i < k; ++i) {
        x[i] = static_cast<double>(ts[knotIdx[i]] - tBase) * 1e-6;
        yv[i] = values[knotIdx[i]];
    }

    // Solve for second derivatives m[] of the natural cubic spline via the
    // Thomas algorithm on the standard tridiagonal system (O(k)).
    std::vector<double> m(k, 0.0);   // second derivatives; natural: m[0]=m[k-1]=0
    std::vector<double> cp(k, 0.0);  // scratch: modified superdiagonal
    std::vector<double> dp(k, 0.0);  // scratch: modified rhs
    for (size_t i = 1; i + 1 < k; ++i) {
        double h0 = x[i] - x[i - 1];
        double h1 = x[i + 1] - x[i];
        if (h0 <= 0.0 || h1 <= 0.0) {
            // Duplicate/non-increasing knot timestamps — spline undefined;
            // fall back to linear which handles dt==0.
            return fillLinear();
        }
        double diag = 2.0 * (h0 + h1);
        double rhs = 6.0 * ((yv[i + 1] - yv[i]) / h1 - (yv[i] - yv[i - 1]) / h0);
        double lower = h0;
        double denom = diag - lower * cp[i - 1];
        cp[i] = h1 / denom;
        dp[i] = (rhs - lower * dp[i - 1]) / denom;
    }
    for (size_t i = k - 2; i >= 1; --i) {
        m[i] = dp[i] - cp[i] * m[i + 1];
    }

    // Evaluate the spline at interior NaN positions.  Both the NaN positions
    // and the knots are in timestamp order, so a single cursor suffices.
    std::vector<double> result(values);
    size_t seg = 0;  // knot segment cursor: interval [knotIdx[seg], knotIdx[seg+1]]
    for (size_t i = knotIdx.front() + 1; i < knotIdx.back(); ++i) {
        if (!std::isnan(result[i])) {
            continue;
        }
        while (seg + 2 < k && knotIdx[seg + 1] < i) {
            ++seg;
        }
        const double h = x[seg + 1] - x[seg];
        const double t = static_cast<double>(ts[i] - tBase) * 1e-6;
        const double a = (x[seg + 1] - t) / h;
        const double b = (t - x[seg]) / h;
        result[i] = a * yv[seg] + b * yv[seg + 1] +
                    ((a * a * a - a) * m[seg] + (b * b * b - b) * m[seg + 1]) * (h * h) / 6.0;
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::gaussianSmooth(double sigma) const {
    // Gaussian kernel convolution with NaN-aware renormalization.
    // Kernel radius = ceil(3*sigma) (covers 99.7% of the Gaussian mass).
    // Interior windows with no NaN use a precomputed normalized kernel and a
    // SIMD dot product; boundary/NaN windows renormalize over the valid taps.
    // An all-NaN window yields NaN.
    const size_t n = values.size();
    if (n == 0) {
        return AlignedSeries(timestamps, {});
    }

    const int half = static_cast<int>(std::ceil(3.0 * sigma));
    const int kernelSize = 2 * half + 1;

    // Precompute normalized kernel.
    std::vector<double> kernel(static_cast<size_t>(kernelSize));
    double kernelSum = 0.0;
    const double inv2s2 = 1.0 / (2.0 * sigma * sigma);
    for (int j = 0; j < kernelSize; ++j) {
        const double xj = static_cast<double>(j - half);
        kernel[static_cast<size_t>(j)] = std::exp(-(xj * xj) * inv2s2);
        kernelSum += kernel[static_cast<size_t>(j)];
    }
    for (double& kv : kernel) {
        kv /= kernelSum;
    }

    // NaN prefix counts: nanPrefix[i] = number of NaNs in values[0..i).
    // Lets each window check "any NaN in range" in O(1), so the common
    // NaN-free interior case takes the branch-free dot-product fast path.
    std::vector<uint32_t> nanPrefix(n + 1, 0);
    for (size_t i = 0; i < n; ++i) {
        nanPrefix[i + 1] = nanPrefix[i] + (std::isnan(values[i]) ? 1u : 0u);
    }

    std::vector<double> result(n);
    const int ni = static_cast<int>(n);
    for (int i = 0; i < ni; ++i) {
        const int lo = i - half;
        const int hi = i + half;  // inclusive
        if (lo >= 0 && hi < ni && nanPrefix[static_cast<size_t>(hi) + 1] == nanPrefix[static_cast<size_t>(lo)]) {
            // Full window, no NaN: normalized kernel applies directly.
            result[static_cast<size_t>(i)] =
                simd::SimdAggregator::dotProduct(kernel.data(), values.data() + lo, static_cast<size_t>(kernelSize));
            continue;
        }
        // Boundary or NaN-containing window: accumulate valid taps, renormalize.
        double smoothed = 0.0;
        double weightSum = 0.0;
        const int jLo = std::max(lo, 0);
        const int jHi = std::min(hi, ni - 1);
        for (int j = jLo; j <= jHi; ++j) {
            const double v = values[static_cast<size_t>(j)];
            if (!std::isnan(v)) {
                const double w = kernel[static_cast<size_t>(j - lo)];
                smoothed += v * w;
                weightSum += w;
            }
        }
        result[static_cast<size_t>(i)] =
            (weightSum > 0.0) ? smoothed / weightSum : std::numeric_limits<double>::quiet_NaN();
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::fillValue(double v) const {
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
    // Zero-init construction already sets result[0] = 0.0 (no area before the
    // first point); the loop overwrites every later element.
    std::vector<double> result(values.size());
    double area = 0.0;
    for (size_t i = 1; i < values.size(); ++i) {
        if (ts[i] <= ts[i - 1]) {
            result[i] = area;
            continue;
        }
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

AlignedSeries AlignedSeries::standardize() const {
    // Global z-score: (x - mean) / stddev over all non-NaN values.
    // SIMD: one masked sum+count pass, one masked M2 pass, one scale-shift pass.
    // Constant series (stddev == 0) → all zeros; NaN passes through.
    double mean = 0.0, stddev = 0.0;
    size_t valid = transform::simd::mean_stddev_skipnan(values, mean, stddev);
    if (valid == 0) {
        return AlignedSeries(timestamps, std::vector<double>(values));  // all-NaN stays all-NaN
    }
    if (stddev == 0.0) {
        // Constant series: zeros for finite values, NaN passthrough.
        // (v - mean) * 0.0 achieves exactly that: 0 for finite, NaN for NaN.
        return AlignedSeries(timestamps, transform::simd::scale_shift(values, mean, 0.0));
    }
    return AlignedSeries(timestamps, transform::simd::scale_shift(values, mean, 1.0 / stddev));
}

// ==================== Percent of Total ====================

AlignedSeries AlignedSeries::asPercent(const AlignedSeries& series, const AlignedSeries& total) {
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

AlignedSeries AlignedSeries::clampMin(double minVal) const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::isnan(values[i]) ? values[i] : std::max(values[i], minVal);
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::clampMax(double maxVal) const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::isnan(values[i]) ? values[i] : std::min(values[i], maxVal);
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::cutoffMin(double threshold) const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = (values[i] < threshold) ? std::numeric_limits<double>::quiet_NaN() : values[i];
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::cutoffMax(double threshold) const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = (values[i] > threshold) ? std::numeric_limits<double>::quiet_NaN() : values[i];
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::ratePer(double secondsPerPoint, double scale) const {
    if (values.empty()) {
        return AlignedSeries(timestamps, {});
    }
    const auto& ts = *timestamps;

    // NaN-fill construction: element 0 keeps NaN (first point has no previous);
    // the loop overwrites every later element.
    std::vector<double> result(values.size(), std::numeric_limits<double>::quiet_NaN());

    for (size_t i = 1; i < values.size(); ++i) {
        double value_diff = values[i] - values[i - 1];

        // Compute time delta in seconds from timestamps (nanoseconds)
        if (ts[i] <= ts[i - 1]) {
            result[i] = 0.0;
            continue;
        }
        double dt_seconds = static_cast<double>(ts[i] - ts[i - 1]) / 1e9;

        // Fall back to the provided secondsPerPoint if timestamps yield zero
        if (dt_seconds == 0.0) {
            if (secondsPerPoint == 0.0) {
                result[i] = 0.0;
                continue;
            }
            dt_seconds = secondsPerPoint;
        }

        result[i] = (value_diff / dt_seconds) * scale;
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::perMinute(double secondsPerPoint) const {
    return ratePer(secondsPerPoint, 60.0);
}

AlignedSeries AlignedSeries::perHour(double secondsPerPoint) const {
    return ratePer(secondsPerPoint, 3600.0);
}

// ==================== Rolling Window Functions ====================

AlignedSeries AlignedSeries::rollingAvg(int N) const {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    if (N <= 0) {
        throw EvaluationException("rolling_avg() window size N must be a positive integer");
    }
    std::vector<double> result(values.size(), nan);
    if (values.empty()) {
        return AlignedSeries(timestamps, std::move(result));
    }

    // Kahan-compensated sliding window to prevent floating-point drift
    double running_sum = 0.0;
    double comp = 0.0;  // Kahan compensation term
    int valid_count = 0;
    for (size_t i = 0; i < values.size(); ++i) {
        if (!std::isnan(values[i])) {
            double y = values[i] - comp;
            double t = running_sum + y;
            comp = (t - running_sum) - y;
            running_sum = t;
            valid_count++;
        }
        if (i >= static_cast<size_t>(N)) {
            if (!std::isnan(values[i - N])) {
                double y = -values[i - N] - comp;
                double t = running_sum + y;
                comp = (t - running_sum) - y;
                running_sum = t;
                valid_count--;
            }
        }
        if (i + 1 >= static_cast<size_t>(N)) {
            result[i] = (valid_count > 0) ? running_sum / valid_count : nan;
        }
    }
    return AlignedSeries(timestamps, std::move(result));
}

// ==================== rollingMonotone (shared ring-buffer impl) ====================
//
// O(N) amortized sliding-window min/max using a contiguous ring buffer instead of
// std::deque.  For window size W the ring holds at most W indices, so for W=100
// the buffer is 101 * 8 = 808 bytes -- fits entirely in L1 cache with zero heap
// indirection (vs. the segment-map overhead of std::deque).
//
// Cmp = std::less<double>{}    -> rolling_min  (ascending invariant, front is min)
// Cmp = std::greater<double>{} -> rolling_max  (descending invariant, front is max)
template <typename Cmp>
static AlignedSeries rollingMonotone(const AlignedSeries& s, int N, Cmp cmp) {
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

AlignedSeries AlignedSeries::rollingMin(int N) const {
    if (N <= 0) {
        throw EvaluationException("rolling_min() window size N must be a positive integer");
    }
    return rollingMonotone(*this, N, std::less<double>{});
}

AlignedSeries AlignedSeries::rollingMax(int N) const {
    if (N <= 0) {
        throw EvaluationException("rolling_max() window size N must be a positive integer");
    }
    return rollingMonotone(*this, N, std::greater<double>{});
}

AlignedSeries AlignedSeries::rollingSum(int N) const {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    if (N <= 0) {
        throw EvaluationException("rolling_sum() window size N must be a positive integer");
    }
    std::vector<double> result(values.size(), nan);
    if (values.empty()) {
        return AlignedSeries(timestamps, std::move(result));
    }

    // Kahan-compensated sliding window (same engine as rolling_avg, no divide)
    double running_sum = 0.0;
    double comp = 0.0;
    int valid_count = 0;
    for (size_t i = 0; i < values.size(); ++i) {
        if (!std::isnan(values[i])) {
            double y = values[i] - comp;
            double t = running_sum + y;
            comp = (t - running_sum) - y;
            running_sum = t;
            valid_count++;
        }
        if (i >= static_cast<size_t>(N)) {
            if (!std::isnan(values[i - N])) {
                double y = -values[i - N] - comp;
                double t = running_sum + y;
                comp = (t - running_sum) - y;
                running_sum = t;
                valid_count--;
            }
        }
        if (i + 1 >= static_cast<size_t>(N)) {
            result[i] = (valid_count > 0) ? running_sum : nan;
        }
    }
    return AlignedSeries(timestamps, std::move(result));
}

// Shared sliding sorted-window quantile engine for rolling_median /
// rolling_percentile.  Maintains the window's non-NaN values in a sorted
// vector: insert/evict are O(W) memmoves via lower_bound, which beats
// tree/heap structures for practical window sizes (contiguous memory, no
// per-node allocation).  Quantile uses the same linear-interpolation
// convention as percentile_of_series.
static AlignedSeries rollingQuantileImpl(const AlignedSeries& s, int N, double p, const char* fname) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    if (N <= 0) {
        throw EvaluationException(std::string(fname) + "() window size N must be a positive integer");
    }
    std::vector<double> result(s.values.size(), nan);
    if (s.values.empty()) {
        return AlignedSeries(s.timestamps, std::move(result));
    }

    const auto& v = s.values;
    std::vector<double> window;  // sorted non-NaN values currently in the window
    window.reserve(static_cast<size_t>(N));

    const double frac = p / 100.0;
    for (size_t i = 0; i < v.size(); ++i) {
        if (!std::isnan(v[i])) {
            auto it = std::lower_bound(window.begin(), window.end(), v[i]);
            window.insert(it, v[i]);
        }
        if (i >= static_cast<size_t>(N) && !std::isnan(v[i - N])) {
            auto it = std::lower_bound(window.begin(), window.end(), v[i - N]);
            window.erase(it);  // guaranteed present
        }
        if (i + 1 >= static_cast<size_t>(N) && !window.empty()) {
            const double idx = frac * static_cast<double>(window.size() - 1);
            const size_t lo = static_cast<size_t>(std::floor(idx));
            const size_t hi = lo + 1;
            if (hi >= window.size()) {
                result[i] = window.back();
            } else {
                const double w = idx - static_cast<double>(lo);
                result[i] = window[lo] * (1.0 - w) + window[hi] * w;
            }
        }
    }
    return AlignedSeries(s.timestamps, std::move(result));
}

AlignedSeries AlignedSeries::rollingMedian(int N) const {
    return rollingQuantileImpl(*this, N, 50.0, "rolling_median");
}

AlignedSeries AlignedSeries::rollingPercentile(int N, double p) const {
    return rollingQuantileImpl(*this, N, p, "rolling_percentile");
}

AlignedSeries AlignedSeries::rollingStddev(int N) const {
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

    // NaN values are substituted with 0 so that the Welford add/remove
    // symmetry is maintained (matching the zscore() pattern).
    auto safe = [](double v) { return std::isnan(v) ? 0.0 : v; };

    // Phase 1 lambda: k is variable, so we must divide.
    auto welford_add_p1 = [&](double x) {
        ++k;
        double delta = x - M;
        M += delta / static_cast<double>(k);
        Q += delta * (x - M);
    };

    // Phase 1: fill the window (processes elements 0 .. N-2)
    for (size_t i = 0; i < wsize - 1 && i < values.size(); ++i) {
        welford_add_p1(safe(values[i]));
    }

    // Phase 2: k == wsize throughout -- precompute loop-invariant reciprocals.
    const double inv_wsize = 1.0 / static_cast<double>(wsize);
    const double inv_wsize_m1 = (wsize > 1) ? 1.0 / static_cast<double>(wsize - 1) : 0.0;

    for (size_t i = wsize - 1; i < values.size(); ++i) {
        // Add new right-edge element (k goes wsize-1 -> wsize; use inv_wsize).
        ++k;
        {
            double x = safe(values[i]);
            double delta = x - M;
            M += delta * inv_wsize;  // replaces: M += delta / k  (k == wsize)
            Q += delta * (x - M);
        }

        result[i] = std::sqrt(Q * inv_wsize);  // replaces: sqrt(Q / wsize)

        if (i + 1 < values.size()) {
            // Remove old left-edge element (k goes wsize -> wsize-1).
            double x = safe(values[i - wsize + 1]);
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

AlignedSeries AlignedSeries::holtWinters(double alpha, double beta) const {
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

AlignedSeries AlignedSeries::timeShift(int64_t offsetNs) const {
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

    return evaluateNode(expr, queryResults).intoOwned();
}

ExpressionEvaluator::EvalResult ExpressionEvaluator::evaluateNode(const ExpressionNode& node,
                                                                  const QueryResultMap& queryResults) {
    if (++evalDepth_ > MAX_EVAL_DEPTH) {
        --evalDepth_;
        throw EvaluationException("Expression evaluation depth exceeded limit of " + std::to_string(MAX_EVAL_DEPTH));
    }
    struct DepthGuard {
        int& d;
        ~DepthGuard() { --d; }
    } guard{evalDepth_};

    switch (node.type) {
        case ExprNodeType::QUERY_REF:
            // Borrow directly from the QueryResultMap — no O(N) values copy.
            return EvalResult(&evaluateQueryRef(node.asQueryRef(), queryResults));

        case ExprNodeType::SCALAR:
            return EvalResult(evaluateScalar(node.asScalar(), queryResults));

        case ExprNodeType::BINARY_OP:
            return EvalResult(evaluateBinaryOp(node.asBinaryOp(), queryResults));

        case ExprNodeType::UNARY_OP:
            return EvalResult(evaluateUnaryOp(node.asUnaryOp(), queryResults));

        case ExprNodeType::FUNCTION_CALL:
            return EvalResult(evaluateFunctionCall(node.asFunctionCall(), queryResults));

        case ExprNodeType::TIME_SHIFT_FUNCTION:
            return EvalResult(evaluateTimeShiftFunction(node.asTimeShiftFunction(), queryResults));

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
    // Scalar-literal fast path: dispatch to AlignedSeries scalar ops instead of
    // materializing a full constant series and running the series-series path.
    // Results are bit-identical to the general path (true division, no
    // reciprocal shortcut). Only taken when exactly one side is a literal.
    const bool leftScalar = op.left->type == ExprNodeType::SCALAR;
    const bool rightScalar = op.right->type == ExprNodeType::SCALAR;
    if (leftScalar != rightScalar) {
        const double scalar = (leftScalar ? *op.left : *op.right).asScalar().value;
        auto seriesEval = evaluateNode(leftScalar ? *op.right : *op.left, queryResults);
        const AlignedSeries& series = seriesEval.get();
        switch (op.op) {
            case BinaryOpType::ADD:
                return series + scalar;
            case BinaryOpType::SUBTRACT:
                return rightScalar ? series - scalar : series.rsub(scalar);
            case BinaryOpType::MULTIPLY:
                return series * scalar;
            case BinaryOpType::DIVIDE:
                return rightScalar ? series.divIeee(scalar) : series.rdiv(scalar);
            default:
                throw EvaluationException("Unknown binary operator");
        }
    }

    auto leftEval = evaluateNode(*op.left, queryResults);
    auto rightEval = evaluateNode(*op.right, queryResults);
    const AlignedSeries& left = leftEval.get();
    const AlignedSeries& right = rightEval.get();

    // Verify timestamp alignment for meaningful element-wise operations.
    // When timestamps differ (e.g., a - time_shift(a, '7d')), element-wise
    // arithmetic produces meaningless results since left[i] and right[i]
    // correspond to completely different time points.
    if (left.timestamps && right.timestamps && left.timestamps != right.timestamps) {
        // Different timestamp pointers — check if values actually match.
        // Memoize verified pairs so the deep O(N) compare runs at most once
        // per source-series pair within a single evaluation.
        const void* lp = left.timestamps.get();
        const void* rp = right.timestamps.get();
        auto key = (lp < rp) ? std::make_pair(lp, rp) : std::make_pair(rp, lp);
        if (std::find(verifiedAlignedPairs_.begin(), verifiedAlignedPairs_.end(), key) ==
            verifiedAlignedPairs_.end()) {
            if (*left.timestamps != *right.timestamps) {
                throw EvaluationException(
                    "Binary operation requires aligned timestamps. "
                    "Use time_shift with explicit alignment or apply functions separately.");
            }
            verifiedAlignedPairs_.push_back(key);
        }
    }

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
    auto operandEval = evaluateNode(*op.operand, queryResults);
    const AlignedSeries& operand = operandEval.get();

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
            return operand.monotonicDiff();
        case UnaryOpType::DEFAULT_ZERO:
            return operand.defaultZero();
        case UnaryOpType::COUNT_NONZERO:
            return operand.countNonzero();
        case UnaryOpType::COUNT_NOT_NULL:
            return operand.countNotNull();
        // Counter-rate functions
        case UnaryOpType::RATE:
            return operand.rate();
        case UnaryOpType::IRATE:
            return operand.irate();
        case UnaryOpType::INCREASE:
            return operand.increase();
        // Gap-fill / interpolation functions
        case UnaryOpType::FILL_FORWARD:
            return operand.fillForward();
        case UnaryOpType::FILL_BACKWARD:
            return operand.fillBackward();
        case UnaryOpType::FILL_LINEAR:
            return operand.fillLinear();
        case UnaryOpType::FILL_SPLINE:
            return operand.fillSpline();
        // Accumulation functions
        case UnaryOpType::CUMSUM:
            return operand.cumsum();
        case UnaryOpType::INTEGRAL:
            return operand.integral();
        // Normalization
        case UnaryOpType::NORMALIZE:
            return operand.normalize();
        case UnaryOpType::STANDARDIZE:
            return operand.standardize();
        case UnaryOpType::EXP:
            return operand.exp();
        case UnaryOpType::ROUND:
            return operand.roundNearest();
        case UnaryOpType::SIGN:
            return operand.sign();
        case UnaryOpType::DERIV:
            return operand.deriv();
        case UnaryOpType::DELTA:
            return operand.delta();
        case UnaryOpType::IDELTA:
            return operand.idelta();
        case UnaryOpType::CHANGES:
            return operand.changes();
        case UnaryOpType::RESETS:
            return operand.resets();
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
            auto aEval = evaluateNode(*call.args[0], queryResults);
            auto bEval = evaluateNode(*call.args[1], queryResults);
            return AlignedSeries::min(aEval.get(), bEval.get());
        }

        case FunctionType::MAX: {
            if (call.args.size() != 2) {
                throw EvaluationException("max() requires exactly 2 arguments");
            }
            auto aEval = evaluateNode(*call.args[0], queryResults);
            auto bEval = evaluateNode(*call.args[1], queryResults);
            return AlignedSeries::max(aEval.get(), bEval.get());
        }

        case FunctionType::POW: {
            if (call.args.size() != 2) {
                throw EvaluationException("pow() requires exactly 2 arguments");
            }
            auto baseEval = evaluateNode(*call.args[0], queryResults);
            auto expEval = evaluateNode(*call.args[1], queryResults);
            return AlignedSeries::pow(baseEval.get(), expEval.get());
        }

        case FunctionType::CLAMP: {
            if (call.args.size() != 3) {
                throw EvaluationException("clamp() requires exactly 3 arguments");
            }
            auto valEval = evaluateNode(*call.args[0], queryResults);
            auto minEval = evaluateNode(*call.args[1], queryResults);
            auto maxEval = evaluateNode(*call.args[2], queryResults);
            return AlignedSeries::clamp(valEval.get(), minEval.get(), maxEval.get());
        }

        // Transform functions with scalar argument
        // Note: The second argument is expected to be a scalar (constant series).
        // We extract only values[0] from it; a non-scalar series will use its first value.
        case FunctionType::CLAMP_MIN: {
            if (call.args.size() != 2) {
                throw EvaluationException("clamp_min() requires exactly 2 arguments");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto scalarEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& scalarSeries = scalarEval.get();
            if (scalarSeries.empty()) {
                throw EvaluationException("clamp_min() second argument must be a non-empty scalar");
            }
            return series.clampMin(scalarSeries.values[0]);
        }

        case FunctionType::CLAMP_MAX: {
            if (call.args.size() != 2) {
                throw EvaluationException("clamp_max() requires exactly 2 arguments");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto scalarEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& scalarSeries = scalarEval.get();
            if (scalarSeries.empty()) {
                throw EvaluationException("clamp_max() second argument must be a non-empty scalar");
            }
            return series.clampMax(scalarSeries.values[0]);
        }

        case FunctionType::CUTOFF_MIN: {
            if (call.args.size() != 2) {
                throw EvaluationException("cutoff_min() requires exactly 2 arguments");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto scalarEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& scalarSeries = scalarEval.get();
            if (scalarSeries.empty()) {
                throw EvaluationException("cutoff_min() second argument must be a non-empty scalar");
            }
            return series.cutoffMin(scalarSeries.values[0]);
        }

        case FunctionType::CUTOFF_MAX: {
            if (call.args.size() != 2) {
                throw EvaluationException("cutoff_max() requires exactly 2 arguments");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto scalarEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& scalarSeries = scalarEval.get();
            if (scalarSeries.empty()) {
                throw EvaluationException("cutoff_max() second argument must be a non-empty scalar");
            }
            return series.cutoffMax(scalarSeries.values[0]);
        }

        case FunctionType::PER_MINUTE: {
            if (call.args.size() != 2) {
                throw EvaluationException("per_minute() requires exactly 2 arguments");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto scalarEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& scalarSeries = scalarEval.get();
            if (scalarSeries.empty()) {
                throw EvaluationException("per_minute() second argument must be a non-empty scalar");
            }
            return series.perMinute(scalarSeries.values[0]);
        }

        case FunctionType::PER_HOUR: {
            if (call.args.size() != 2) {
                throw EvaluationException("per_hour() requires exactly 2 arguments");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto scalarEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& scalarSeries = scalarEval.get();
            if (scalarSeries.empty()) {
                throw EvaluationException("per_hour() second argument must be a non-empty scalar");
            }
            return series.perHour(scalarSeries.values[0]);
        }

        // Rolling window functions: second argument is integer window size N
        case FunctionType::ROLLING_AVG: {
            if (call.args.size() != 2) {
                throw EvaluationException("rolling_avg() requires exactly 2 arguments");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto scalarEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& scalarSeries = scalarEval.get();
            if (scalarSeries.empty()) {
                throw EvaluationException("rolling_avg() second argument must be a non-empty scalar");
            }
            int N = static_cast<int>(scalarSeries.values[0]);
            if (N <= 0) {
                throw EvaluationException("rolling_avg() window size N must be a positive integer");
            }
            return series.rollingAvg(N);
        }

        case FunctionType::ROLLING_MIN: {
            if (call.args.size() != 2) {
                throw EvaluationException("rolling_min() requires exactly 2 arguments");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto scalarEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& scalarSeries = scalarEval.get();
            if (scalarSeries.empty()) {
                throw EvaluationException("rolling_min() second argument must be a non-empty scalar");
            }
            int N = static_cast<int>(scalarSeries.values[0]);
            if (N <= 0) {
                throw EvaluationException("rolling_min() window size N must be a positive integer");
            }
            return series.rollingMin(N);
        }

        case FunctionType::ROLLING_MAX: {
            if (call.args.size() != 2) {
                throw EvaluationException("rolling_max() requires exactly 2 arguments");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto scalarEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& scalarSeries = scalarEval.get();
            if (scalarSeries.empty()) {
                throw EvaluationException("rolling_max() second argument must be a non-empty scalar");
            }
            int N = static_cast<int>(scalarSeries.values[0]);
            if (N <= 0) {
                throw EvaluationException("rolling_max() window size N must be a positive integer");
            }
            return series.rollingMax(N);
        }

        case FunctionType::ROLLING_STDDEV: {
            if (call.args.size() != 2) {
                throw EvaluationException("rolling_stddev() requires exactly 2 arguments");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto scalarEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& scalarSeries = scalarEval.get();
            if (scalarSeries.empty()) {
                throw EvaluationException("rolling_stddev() second argument must be a non-empty scalar");
            }
            int N = static_cast<int>(scalarSeries.values[0]);
            if (N <= 0) {
                throw EvaluationException("rolling_stddev() window size N must be a positive integer");
            }
            return series.rollingStddev(N);
        }

        case FunctionType::ROLLING_SUM: {
            if (call.args.size() != 2) {
                throw EvaluationException("rolling_sum() requires exactly 2 arguments");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto scalarEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& scalarSeries = scalarEval.get();
            if (scalarSeries.empty()) {
                throw EvaluationException("rolling_sum() second argument must be a non-empty scalar");
            }
            int N = static_cast<int>(scalarSeries.values[0]);
            if (N <= 0) {
                throw EvaluationException("rolling_sum() window size N must be a positive integer");
            }
            return series.rollingSum(N);
        }

        case FunctionType::ROLLING_MEDIAN: {
            if (call.args.size() != 2) {
                throw EvaluationException("rolling_median() requires exactly 2 arguments");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto scalarEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& scalarSeries = scalarEval.get();
            if (scalarSeries.empty()) {
                throw EvaluationException("rolling_median() second argument must be a non-empty scalar");
            }
            int N = static_cast<int>(scalarSeries.values[0]);
            if (N <= 0) {
                throw EvaluationException("rolling_median() window size N must be a positive integer");
            }
            return series.rollingMedian(N);
        }

        case FunctionType::ROLLING_PERCENTILE: {
            // Syntax: rolling_percentile(series, N, p)
            if (call.args.size() != 3) {
                throw EvaluationException("rolling_percentile() requires exactly 3 arguments (series, N, p)");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto nEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& nSeries = nEval.get();
            auto pEval = evaluateNode(*call.args[2], queryResults);
            const AlignedSeries& pSeries = pEval.get();
            if (nSeries.empty() || pSeries.empty()) {
                throw EvaluationException("rolling_percentile() N and p arguments must be non-empty scalars");
            }
            int N = static_cast<int>(nSeries.values[0]);
            if (N <= 0) {
                throw EvaluationException("rolling_percentile() window size N must be a positive integer");
            }
            double p = pSeries.values[0];
            if (p < 0.0 || p > 100.0) {
                throw EvaluationException("rolling_percentile() p must be in [0, 100], got " + std::to_string(p));
            }
            return series.rollingPercentile(N, p);
        }

        case FunctionType::FILL_VALUE: {
            if (call.args.size() != 2) {
                throw EvaluationException("fill_value() requires exactly 2 arguments");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto scalarEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& scalarSeries = scalarEval.get();
            if (scalarSeries.empty()) {
                throw EvaluationException("fill_value() second argument must be a non-empty scalar");
            }
            return series.fillValue(scalarSeries.values[0]);
        }

        case FunctionType::EMA: {
            if (call.args.size() != 2) {
                throw EvaluationException("ema() requires exactly 2 arguments");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto scalarEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& scalarSeries = scalarEval.get();
            if (scalarSeries.empty()) {
                throw EvaluationException("ema() second argument must be a non-empty scalar");
            }
            double param = scalarSeries.values[0];
            if (param <= 0.0) {
                throw EvaluationException("ema() parameter must be positive (alpha in (0,1] or span > 1)");
            }
            return series.ema(param);
        }

        case FunctionType::GAUSSIAN_SMOOTH: {
            if (call.args.size() != 2) {
                throw EvaluationException("gaussian_smooth() requires exactly 2 arguments");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto scalarEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& scalarSeries = scalarEval.get();
            if (scalarSeries.empty()) {
                throw EvaluationException("gaussian_smooth() second argument must be a non-empty scalar");
            }
            double sigma = scalarSeries.values[0];
            if (!(sigma > 0.0) || sigma > 1e6) {
                throw EvaluationException("gaussian_smooth() sigma must be in (0, 1e6]");
            }
            return series.gaussianSmooth(sigma);
        }

        case FunctionType::HOLT_WINTERS: {
            if (call.args.size() != 3) {
                throw EvaluationException("holt_winters() requires exactly 3 arguments");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto alphaEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& alphaSeries = alphaEval.get();
            auto betaEval = evaluateNode(*call.args[2], queryResults);
            const AlignedSeries& betaSeries = betaEval.get();
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
            return series.holtWinters(alpha, beta);
        }

        case FunctionType::ZSCORE: {
            if (call.args.size() != 2) {
                throw EvaluationException("zscore() requires exactly 2 arguments");
            }
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& series = seriesEval.get();
            auto scalarEval = evaluateNode(*call.args[1], queryResults);
            const AlignedSeries& scalarSeries = scalarEval.get();
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
            auto seriesEval = evaluateNode(*call.args[0], queryResults);
            auto totalEval = evaluateNode(*call.args[1], queryResults);
            return AlignedSeries::asPercent(seriesEval.get(), totalEval.get());
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
            auto scalarEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& scalarSeries = scalarEval.get();
            if (scalarSeries.empty()) {
                const char* fname = (call.func == FunctionType::TOPK) ? "topk" : "bottomk";
                throw EvaluationException(std::string(fname) + "() first argument (N) must be a non-empty scalar");
            }
            int N = static_cast<int>(scalarSeries.values[0]);
            if (N < 0) {
                const char* fname = (call.func == FunctionType::TOPK) ? "topk" : "bottomk";
                throw EvaluationException(std::string(fname) + "() N must be non-negative");
            }
            auto seriesEval = evaluateNode(*call.args[1], queryResults);
            // In single-series context there is exactly one "group".
            // If N >= 1, the group is kept; if N == 0, return an empty series.
            if (N == 0) {
                return AlignedSeries(std::vector<uint64_t>{}, std::vector<double>{});
            }
            return std::move(seriesEval).intoOwned();  // N >= 1: single group is always kept
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
            std::vector<EvalResult> seriesEvals;
            seriesEvals.reserve(call.args.size());
            for (const auto& arg : call.args) {
                seriesEvals.push_back(evaluateNode(*arg, queryResults));
            }
            std::vector<const AlignedSeries*> series;
            series.reserve(seriesEvals.size());
            for (const auto& ev : seriesEvals) {
                series.push_back(&ev.get());
            }
            // Verify sizes match
            size_t n = series[0]->size();
            for (size_t i = 1; i < series.size(); ++i) {
                if (series[i]->size() != n) {
                    throw EvaluationException("avg_of_series(): all series must have the same length");
                }
            }
            const double nan = std::numeric_limits<double>::quiet_NaN();
            std::vector<double> result(n, nan);
            for (size_t i = 0; i < n; ++i) {
                double sum = 0.0;
                size_t cnt = 0;
                for (const auto& s : series) {
                    if (!std::isnan(s->values[i])) {
                        sum += s->values[i];
                        ++cnt;
                    }
                }
                if (cnt > 0)
                    result[i] = sum / static_cast<double>(cnt);
            }
            return AlignedSeries(series[0]->timestamps, std::move(result));
        }

        case FunctionType::SUM_OF_SERIES: {
            if (call.args.empty()) {
                throw EvaluationException("sum_of_series() requires at least 1 argument");
            }
            std::vector<EvalResult> seriesEvals;
            seriesEvals.reserve(call.args.size());
            for (const auto& arg : call.args) {
                seriesEvals.push_back(evaluateNode(*arg, queryResults));
            }
            std::vector<const AlignedSeries*> series;
            series.reserve(seriesEvals.size());
            for (const auto& ev : seriesEvals) {
                series.push_back(&ev.get());
            }
            size_t n = series[0]->size();
            for (size_t i = 1; i < series.size(); ++i) {
                if (series[i]->size() != n) {
                    throw EvaluationException("sum_of_series(): all series must have the same length");
                }
            }
            const double nan = std::numeric_limits<double>::quiet_NaN();
            std::vector<double> result(n, nan);
            for (size_t i = 0; i < n; ++i) {
                double sum = 0.0;
                size_t cnt = 0;
                for (const auto& s : series) {
                    if (!std::isnan(s->values[i])) {
                        sum += s->values[i];
                        ++cnt;
                    }
                }
                if (cnt > 0)
                    result[i] = sum;
            }
            return AlignedSeries(series[0]->timestamps, std::move(result));
        }

        case FunctionType::MIN_OF_SERIES: {
            if (call.args.empty()) {
                throw EvaluationException("min_of_series() requires at least 1 argument");
            }
            std::vector<EvalResult> seriesEvals;
            seriesEvals.reserve(call.args.size());
            for (const auto& arg : call.args) {
                seriesEvals.push_back(evaluateNode(*arg, queryResults));
            }
            std::vector<const AlignedSeries*> series;
            series.reserve(seriesEvals.size());
            for (const auto& ev : seriesEvals) {
                series.push_back(&ev.get());
            }
            size_t n = series[0]->size();
            for (size_t i = 1; i < series.size(); ++i) {
                if (series[i]->size() != n) {
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
                    if (!std::isnan(s->values[i])) {
                        if (!found || s->values[i] < minVal) {
                            minVal = s->values[i];
                            found = true;
                        }
                    }
                }
                if (found)
                    result[i] = minVal;
            }
            return AlignedSeries(series[0]->timestamps, std::move(result));
        }

        case FunctionType::MAX_OF_SERIES: {
            if (call.args.empty()) {
                throw EvaluationException("max_of_series() requires at least 1 argument");
            }
            std::vector<EvalResult> seriesEvals;
            seriesEvals.reserve(call.args.size());
            for (const auto& arg : call.args) {
                seriesEvals.push_back(evaluateNode(*arg, queryResults));
            }
            std::vector<const AlignedSeries*> series;
            series.reserve(seriesEvals.size());
            for (const auto& ev : seriesEvals) {
                series.push_back(&ev.get());
            }
            size_t n = series[0]->size();
            for (size_t i = 1; i < series.size(); ++i) {
                if (series[i]->size() != n) {
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
                    if (!std::isnan(s->values[i])) {
                        if (!found || s->values[i] > maxVal) {
                            maxVal = s->values[i];
                            found = true;
                        }
                    }
                }
                if (found)
                    result[i] = maxVal;
            }
            return AlignedSeries(series[0]->timestamps, std::move(result));
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
            auto pEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& pSeries = pEval.get();
            if (pSeries.empty()) {
                throw EvaluationException("percentile_of_series() first argument (p) must be a non-empty scalar");
            }
            double p = pSeries.values[0];
            if (p < 0.0 || p > 100.0) {
                throw EvaluationException("percentile_of_series() p must be in [0, 100], got " + std::to_string(p));
            }

            // Evaluate the series arguments
            std::vector<EvalResult> seriesEvals;
            seriesEvals.reserve(call.args.size() - 1);
            for (size_t a = 1; a < call.args.size(); ++a) {
                seriesEvals.push_back(evaluateNode(*call.args[a], queryResults));
            }
            std::vector<const AlignedSeries*> series;
            series.reserve(seriesEvals.size());
            for (const auto& ev : seriesEvals) {
                series.push_back(&ev.get());
            }
            size_t n = series[0]->size();
            for (size_t i = 1; i < series.size(); ++i) {
                if (series[i]->size() != n) {
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
                    if (!std::isnan(s->values[i])) {
                        tmp.push_back(s->values[i]);
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
            return AlignedSeries(series[0]->timestamps, std::move(result));
        }

        case FunctionType::COUNT_OF_SERIES: {
            if (call.args.empty()) {
                throw EvaluationException("count_of_series() requires at least 1 argument");
            }
            std::vector<EvalResult> seriesEvals;
            seriesEvals.reserve(call.args.size());
            for (const auto& arg : call.args) {
                seriesEvals.push_back(evaluateNode(*arg, queryResults));
            }
            std::vector<const AlignedSeries*> series;
            series.reserve(seriesEvals.size());
            for (const auto& ev : seriesEvals) {
                series.push_back(&ev.get());
            }
            size_t n = series[0]->size();
            for (size_t i = 1; i < series.size(); ++i) {
                if (series[i]->size() != n) {
                    throw EvaluationException("count_of_series(): all series must have the same length");
                }
            }
            // Count of non-NaN values at each index; 0 when nothing reports
            // ("how many hosts are reporting" wants 0, not NaN).
            std::vector<double> result(n, 0.0);
            for (const auto& s : series) {
                for (size_t i = 0; i < n; ++i) {
                    if (!std::isnan(s->values[i])) {
                        result[i] += 1.0;
                    }
                }
            }
            return AlignedSeries(series[0]->timestamps, std::move(result));
        }

        case FunctionType::STDDEV_OF_SERIES: {
            if (call.args.empty()) {
                throw EvaluationException("stddev_of_series() requires at least 1 argument");
            }
            std::vector<EvalResult> seriesEvals;
            seriesEvals.reserve(call.args.size());
            for (const auto& arg : call.args) {
                seriesEvals.push_back(evaluateNode(*arg, queryResults));
            }
            std::vector<const AlignedSeries*> series;
            series.reserve(seriesEvals.size());
            for (const auto& ev : seriesEvals) {
                series.push_back(&ev.get());
            }
            size_t n = series[0]->size();
            for (size_t i = 1; i < series.size(); ++i) {
                if (series[i]->size() != n) {
                    throw EvaluationException("stddev_of_series(): all series must have the same length");
                }
            }
            // Population stddev across non-NaN values at each index.
            // No values → NaN; a single value → 0 (PromQL convention).
            const double nan = std::numeric_limits<double>::quiet_NaN();
            std::vector<double> result(n, nan);
            for (size_t i = 0; i < n; ++i) {
                double sum = 0.0;
                size_t count = 0;
                for (const auto& s : series) {
                    if (!std::isnan(s->values[i])) {
                        sum += s->values[i];
                        ++count;
                    }
                }
                if (count == 0)
                    continue;
                const double mean = sum / static_cast<double>(count);
                double m2 = 0.0;
                for (const auto& s : series) {
                    if (!std::isnan(s->values[i])) {
                        const double d = s->values[i] - mean;
                        m2 += d * d;
                    }
                }
                result[i] = std::sqrt(m2 / static_cast<double>(count));
            }
            return AlignedSeries(series[0]->timestamps, std::move(result));
        }

        case FunctionType::HISTOGRAM_QUANTILE: {
            // histogram_quantile(p, le_1, b_1, ..., le_n, b_n, b_inf)
            //
            // PromQL-style quantile estimation from cumulative histogram
            // buckets.  Each b_k is a series of cumulative counts of
            // observations <= le_k; b_inf is the +Inf bucket (total count).
            // Bounds are scalar literals, strictly ascending and finite.
            //
            // Per timestamp: counts are clamped to monotonic non-decreasing
            // (scrape/rollup artifacts), rank = (p/100) * total locates the
            // target bucket, and the quantile is linearly interpolated within
            // it.  The first bucket interpolates from 0 when its bound is
            // positive; a rank falling in the +Inf bucket returns the highest
            // finite bound; total <= 0 or a NaN bucket value yields NaN.
            if (call.args.size() < 4 || call.args.size() % 2 != 0) {
                throw EvaluationException(
                    "histogram_quantile() expects p, (le, bucket) pairs, and the +Inf bucket "
                    "(even argument count >= 4)");
            }
            auto pEval = evaluateNode(*call.args[0], queryResults);
            const AlignedSeries& pSeries = pEval.get();
            if (pSeries.empty()) {
                throw EvaluationException("histogram_quantile() first argument (p) must be a non-empty scalar");
            }
            const double p = pSeries.values[0];
            if (p < 0.0 || p > 100.0) {
                throw EvaluationException("histogram_quantile() p must be in [0, 100], got " + std::to_string(p));
            }
            const double phi = p / 100.0;

            // Decode (bound, bucket) pairs + trailing +Inf bucket.
            const size_t numFinite = (call.args.size() - 2) / 2;
            std::vector<double> bounds(numFinite);
            std::vector<EvalResult> buckets;
            buckets.reserve(numFinite + 1);
            for (size_t k = 0; k < numFinite; ++k) {
                auto boundEval = evaluateNode(*call.args[1 + 2 * k], queryResults);
                const AlignedSeries& boundSeries = boundEval.get();
                if (boundSeries.empty()) {
                    throw EvaluationException("histogram_quantile() bucket bound must be a non-empty scalar");
                }
                bounds[k] = boundSeries.values[0];
                if (!std::isfinite(bounds[k])) {
                    throw EvaluationException("histogram_quantile() bucket bounds must be finite");
                }
                if (k > 0 && bounds[k] <= bounds[k - 1]) {
                    throw EvaluationException("histogram_quantile() bucket bounds must be strictly ascending");
                }
                buckets.push_back(evaluateNode(*call.args[2 + 2 * k], queryResults));
            }
            buckets.push_back(evaluateNode(*call.args.back(), queryResults));  // +Inf bucket

            const size_t n = buckets[0].get().size();
            for (size_t k = 1; k < buckets.size(); ++k) {
                if (buckets[k].get().size() != n) {
                    throw EvaluationException("histogram_quantile(): all bucket series must have the same length");
                }
            }

            const double nan = std::numeric_limits<double>::quiet_NaN();
            std::vector<double> result(n, nan);
            std::vector<double> counts(buckets.size());
            for (size_t i = 0; i < n; ++i) {
                // Gather cumulative counts; any NaN bucket invalidates this point.
                bool valid = true;
                for (size_t k = 0; k < buckets.size(); ++k) {
                    counts[k] = buckets[k].get().values[i];
                    if (std::isnan(counts[k])) {
                        valid = false;
                        break;
                    }
                }
                if (!valid) {
                    continue;
                }
                // Clamp to monotonic non-decreasing (PromQL tolerance for
                // scrape artifacts / float noise in rollups).
                for (size_t k = 1; k < counts.size(); ++k) {
                    if (counts[k] < counts[k - 1]) {
                        counts[k] = counts[k - 1];
                    }
                }
                const double total = counts.back();
                if (!(total > 0.0)) {
                    continue;  // empty histogram: NaN
                }
                const double rank = phi * total;

                // First finite bucket whose cumulative count reaches the rank.
                size_t k = 0;
                while (k < numFinite && counts[k] < rank) {
                    ++k;
                }
                if (k == numFinite) {
                    // Rank falls in the +Inf bucket: the quantile is beyond
                    // the highest finite bound — return that bound (PromQL).
                    result[i] = bounds[numFinite - 1];
                    continue;
                }
                const double upper = bounds[k];
                const double lower = (k == 0) ? (upper > 0.0 ? 0.0 : upper) : bounds[k - 1];
                const double prevCount = (k == 0) ? 0.0 : counts[k - 1];
                const double bucketCount = counts[k] - prevCount;
                if (bucketCount <= 0.0) {
                    result[i] = upper;
                } else {
                    result[i] = lower + (upper - lower) * (rank - prevCount) / bucketCount;
                }
            }
            return AlignedSeries(buckets[0].get().timestamps, std::move(result));
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

    return series.timeShift(offsetNs);
}

}  // namespace timestar
