#pragma once

#include "derived_query.hpp"
#include "expression_ast.hpp"

#include <algorithm>
#include <cmath>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace timestar {

// Exception for evaluation errors
class EvaluationException : public std::runtime_error {
public:
    explicit EvaluationException(const std::string& message) : std::runtime_error(message) {}
};

// A time series that has been aligned (same timestamps across all series)
// NOTE: the /derived DSL function names are snake_case by design (e.g. "fill_forward",
// "rolling_avg"); the parser maps them to these camelBack C++ methods via enum dispatch.
struct AlignedSeries {
    // Shared ownership of the timestamp buffer — O(1) copy for element-wise ops.
    std::shared_ptr<const std::vector<uint64_t>> timestamps;
    std::vector<double> values;

    // Default constructor: empty series.
    AlignedSeries() : timestamps(std::make_shared<const std::vector<uint64_t>>()) {}

    // Primary constructor: takes ownership of ts by moving it into a new shared_ptr.
    AlignedSeries(std::vector<uint64_t> ts, std::vector<double> vals)
        : timestamps(std::make_shared<const std::vector<uint64_t>>(std::move(ts))), values(std::move(vals)) {
        if (timestamps->size() != values.size()) {
            throw EvaluationException("Timestamp and value vectors must have the same size");
        }
    }

    // Shared-pointer constructor: O(1) copy of the pointer — used by element-wise ops.
    AlignedSeries(std::shared_ptr<const std::vector<uint64_t>> ts_ptr, std::vector<double> vals)
        : timestamps(std::move(ts_ptr)), values(std::move(vals)) {
        if (timestamps->size() != values.size()) {
            throw EvaluationException("Timestamp and value vectors must have the same size");
        }
    }

    // Move/copy constructors and assignment (defaulted).
    AlignedSeries(AlignedSeries&&) = default;
    AlignedSeries& operator=(AlignedSeries&&) = default;
    AlignedSeries(const AlignedSeries&) = default;
    AlignedSeries& operator=(const AlignedSeries&) = default;

    bool empty() const { return timestamps->empty(); }
    size_t size() const { return timestamps->size(); }

    // Element-wise operations
    AlignedSeries operator+(const AlignedSeries& other) const;
    AlignedSeries operator-(const AlignedSeries& other) const;
    AlignedSeries operator*(const AlignedSeries& other) const;
    AlignedSeries operator/(const AlignedSeries& other) const;

    // Scalar operations
    AlignedSeries operator+(double scalar) const;
    AlignedSeries operator-(double scalar) const;
    AlignedSeries operator*(double scalar) const;
    AlignedSeries operator/(double scalar) const;

    // Scalar-left / IEEE variants (used by the evaluator's scalar-literal fast
    // path; bit-identical to element-wise ops against a constant series):
    AlignedSeries rsub(double scalar) const;     // scalar - values[i]
    AlignedSeries rdiv(double scalar) const;     // scalar / values[i] (IEEE: /0 -> Inf/NaN)
    AlignedSeries divIeee(double scalar) const;  // values[i] / scalar (IEEE, no throw, true divide)

    // Unary operations
    AlignedSeries negate() const;
    AlignedSeries abs() const;
    AlignedSeries log() const;
    AlignedSeries log10() const;
    AlignedSeries sqrt() const;
    AlignedSeries ceil() const;
    AlignedSeries floor() const;
    AlignedSeries exp() const;           // e^x (SIMD); NaN passthrough
    AlignedSeries roundNearest() const;  // Nearest integer, halves away from zero (SIMD); NaN passthrough
    AlignedSeries sign() const;          // -1/0/+1 (SIMD); NaN passthrough

    // Transform functions (unary)
    AlignedSeries diff() const;           // Difference between consecutive points
    AlignedSeries monotonicDiff() const;  // Diff with counter reset handling
    AlignedSeries defaultZero() const;    // Replace NaN with 0
    AlignedSeries countNonzero() const;   // Count non-zero values (returns scalar series)
    AlignedSeries countNotNull() const;   // Count non-NaN values (returns scalar series)

    // Counter-rate functions (require access to timestamps)
    AlignedSeries rate() const;      // Per-second rate; handles counter resets; first point NaN
    AlignedSeries irate() const;     // Instantaneous rate using last two points (constant series)
    AlignedSeries increase() const;  // Total increase over the series (sum of positive diffs, scalar)
    // Gauge derivative / range summaries
    AlignedSeries deriv() const;    // Per-second first derivative (SIMD); negatives allowed; first point NaN
    AlignedSeries delta() const;    // Last non-NaN minus first non-NaN (constant series; NaN if no data)
    AlignedSeries idelta() const;   // Difference of last two non-NaN values (constant series)
    AlignedSeries changes() const;  // Count of value changes between consecutive non-NaN points (constant series)
    AlignedSeries resets() const;   // Count of decreases between consecutive non-NaN points (constant series)

    // Gap-fill / interpolation functions
    AlignedSeries fillForward() const;   // LOCF: replace NaN with previous non-NaN; leading NaNs stay NaN
    AlignedSeries fillBackward() const;  // NOCB: replace NaN with next non-NaN; trailing NaNs stay NaN
    AlignedSeries fillLinear() const;    // Linear interpolation using timestamps; leading/trailing NaN runs stay NaN
    AlignedSeries fillSpline() const;    // Natural cubic spline through known values; leading/trailing NaN stay NaN
    AlignedSeries fillValue(double v) const;  // Replace every NaN with constant v
    // Gaussian kernel smoothing (radius ceil(3*sigma)); NaN-aware renormalization
    AlignedSeries gaussianSmooth(double sigma) const;

    // Accumulation functions
    AlignedSeries cumsum() const;    // Running cumulative sum; NaN treated as 0 (skip-NaN)
    AlignedSeries integral() const;  // Definite integral via trapezoidal rule; NaN trapezoids contribute 0

    // Normalization
    AlignedSeries normalize() const;    // Rescale to [0,1]; constant or single-point → 0.0; NaN passthrough
    AlignedSeries standardize() const;  // Global z-score (SIMD); constant series → 0.0; NaN passthrough

    // Binary functions
    static AlignedSeries min(const AlignedSeries& a, const AlignedSeries& b);
    static AlignedSeries max(const AlignedSeries& a, const AlignedSeries& b);
    static AlignedSeries pow(const AlignedSeries& base, const AlignedSeries& exp);
    static AlignedSeries clamp(const AlignedSeries& val, const AlignedSeries& minVal, const AlignedSeries& maxVal);

    // Transform functions with scalar argument
    AlignedSeries clampMin(double minVal) const;      // Clamp values to minimum
    AlignedSeries clampMax(double maxVal) const;      // Clamp values to maximum
    AlignedSeries cutoffMin(double threshold) const;  // Set values below threshold to NaN
    AlignedSeries cutoffMax(double threshold) const;  // Set values above threshold to NaN
    AlignedSeries ratePer(double secondsPerPoint, double scale) const;
    AlignedSeries perMinute(double secondsPerPoint) const;  // Rate * 60
    AlignedSeries perHour(double secondsPerPoint) const;    // Rate * 3600

    // Percent of total (element-wise): 100 * this[i] / total[i]; NaN on div-by-zero or NaN input
    static AlignedSeries asPercent(const AlignedSeries& series, const AlignedSeries& total);

    // Rolling window functions (N-point window; first N-1 points output NaN)
    AlignedSeries rollingAvg(int N) const;                   // N-point simple moving average
    AlignedSeries rollingMin(int N) const;                   // N-point rolling minimum
    AlignedSeries rollingMax(int N) const;                   // N-point rolling maximum
    AlignedSeries rollingStddev(int N) const;                // N-point rolling population stddev (ddof=0)
    AlignedSeries rollingSum(int N) const;                   // N-point rolling sum (Kahan-compensated)
    AlignedSeries rollingMedian(int N) const;                // N-point rolling median (sorted sliding window)
    AlignedSeries rollingPercentile(int N, double p) const;  // N-point rolling p-th percentile, p in [0,100]
    AlignedSeries zscore(int N) const;  // N-point rolling z-score: (v - mean) / stddev; 0 if stddev==0

    // Exponential moving average
    // param <= 1.0: treated as alpha directly; param > 1.0: treated as span N, alpha = 2/(N+1)
    // Leading NaN inputs remain NaN; internal NaN inputs carry forward the last EMA value
    AlignedSeries ema(double param) const;

    // Double exponential smoothing (Holt's linear method)
    // alpha in (0,1]: level smoothing factor
    // beta  in (0,1]: trend smoothing factor
    // Leading NaN inputs remain NaN; NaN inputs inside the series carry forward level/trend
    AlignedSeries holtWinters(double alpha, double beta) const;

    // Timestamp shift: add offsetNs (may be negative) to every timestamp
    // Values are unchanged; timestamps are shifted by exactly offsetNs nanoseconds.
    AlignedSeries timeShift(int64_t offsetNs) const;
};

// Evaluates expression ASTs against aligned time series data
class ExpressionEvaluator {
public:
    // Map of query name to aligned series data
    using QueryResultMap = std::map<std::string, AlignedSeries>;

    ExpressionEvaluator() = default;

    // Evaluate an expression against the provided query results
    // All series in queryResults must already be aligned (same timestamps)
    AlignedSeries evaluate(const ExpressionNode& expr, const QueryResultMap& queryResults);

private:
    static constexpr int MAX_EVAL_DEPTH = 100;
    int evalDepth_ = 0;

    // Borrowed-or-owned evaluation result. QUERY_REF leaves borrow the series
    // straight from the QueryResultMap (no O(N) values copy); computed nodes
    // own their result. Borrowed pointers reference the caller-owned map, which
    // outlives the entire evaluation.
    class EvalResult {
    public:
        explicit EvalResult(const AlignedSeries* borrowed) : borrowed_(borrowed) {}
        explicit EvalResult(AlignedSeries&& owned) : owned_(std::move(owned)) {}

        EvalResult(EvalResult&&) = default;
        EvalResult& operator=(EvalResult&&) = default;

        const AlignedSeries& get() const { return borrowed_ ? *borrowed_ : owned_; }

        // Convert to an owned AlignedSeries (copies only when borrowed).
        AlignedSeries intoOwned() && { return borrowed_ ? *borrowed_ : std::move(owned_); }

    private:
        const AlignedSeries* borrowed_ = nullptr;
        AlignedSeries owned_;
    };

    EvalResult evaluateNode(const ExpressionNode& node, const QueryResultMap& queryResults);

    // Timestamp-vector pairs already verified element-equal by a binary op.
    // Avoids repeated O(N) deep compares when the same two source series
    // appear multiple times in one formula (e.g. "(a - b) / (a + b)").
    std::vector<std::pair<const void*, const void*>> verifiedAlignedPairs_;

    const AlignedSeries& evaluateQueryRef(const QueryRef& ref, const QueryResultMap& queryResults);

    AlignedSeries evaluateScalar(const ScalarValue& scalar, const QueryResultMap& queryResults);

    AlignedSeries evaluateBinaryOp(const BinaryOp& op, const QueryResultMap& queryResults);

    AlignedSeries evaluateUnaryOp(const UnaryOp& op, const QueryResultMap& queryResults);

    AlignedSeries evaluateFunctionCall(const FunctionCall& call, const QueryResultMap& queryResults);

    AlignedSeries evaluateTimeShiftFunction(const TimeShiftFunction& ts, const QueryResultMap& queryResults);

    // Create a scalar series (same value at all timestamps)
    AlignedSeries makeScalarSeries(double value, const std::shared_ptr<const std::vector<uint64_t>>& tsPtr) const;

    // Get the reference timestamps from query results
    std::shared_ptr<const std::vector<uint64_t>> getReferenceTimestamps(const QueryResultMap& queryResults) const;
};

}  // namespace timestar
