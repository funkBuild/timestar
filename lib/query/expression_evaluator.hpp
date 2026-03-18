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

    // Unary operations
    AlignedSeries negate() const;
    AlignedSeries abs() const;
    AlignedSeries log() const;
    AlignedSeries log10() const;
    AlignedSeries sqrt() const;
    AlignedSeries ceil() const;
    AlignedSeries floor() const;

    // Transform functions (unary)
    AlignedSeries diff() const;            // Difference between consecutive points
    AlignedSeries monotonic_diff() const;  // Diff with counter reset handling
    AlignedSeries default_zero() const;    // Replace NaN with 0
    AlignedSeries count_nonzero() const;   // Count non-zero values (returns scalar series)
    AlignedSeries count_not_null() const;  // Count non-NaN values (returns scalar series)

    // Counter-rate functions (require access to timestamps)
    AlignedSeries rate() const;      // Per-second rate; handles counter resets; first point NaN
    AlignedSeries irate() const;     // Instantaneous rate using last two points (constant series)
    AlignedSeries increase() const;  // Total increase over the series (sum of positive diffs, scalar)

    // Gap-fill / interpolation functions
    AlignedSeries fill_forward() const;   // LOCF: replace NaN with previous non-NaN; leading NaNs stay NaN
    AlignedSeries fill_backward() const;  // NOCB: replace NaN with next non-NaN; trailing NaNs stay NaN
    AlignedSeries fill_linear() const;    // Linear interpolation using timestamps; leading/trailing NaN runs stay NaN
    AlignedSeries fill_value(double v) const;  // Replace every NaN with constant v

    // Accumulation functions
    AlignedSeries cumsum() const;    // Running cumulative sum; NaN treated as 0 (skip-NaN)
    AlignedSeries integral() const;  // Definite integral via trapezoidal rule; NaN trapezoids contribute 0

    // Normalization
    AlignedSeries normalize() const;  // Rescale to [0,1]; constant or single-point → 0.0; NaN passthrough

    // Binary functions
    static AlignedSeries min(const AlignedSeries& a, const AlignedSeries& b);
    static AlignedSeries max(const AlignedSeries& a, const AlignedSeries& b);
    static AlignedSeries pow(const AlignedSeries& base, const AlignedSeries& exp);
    static AlignedSeries clamp(const AlignedSeries& val, const AlignedSeries& minVal, const AlignedSeries& maxVal);

    // Transform functions with scalar argument
    AlignedSeries clamp_min(double minVal) const;      // Clamp values to minimum
    AlignedSeries clamp_max(double maxVal) const;      // Clamp values to maximum
    AlignedSeries cutoff_min(double threshold) const;  // Set values below threshold to NaN
    AlignedSeries cutoff_max(double threshold) const;  // Set values above threshold to NaN
    AlignedSeries rate_per(double seconds_per_point, double scale) const;
    AlignedSeries per_minute(double seconds_per_point) const;  // Rate * 60
    AlignedSeries per_hour(double seconds_per_point) const;    // Rate * 3600

    // Percent of total (element-wise): 100 * this[i] / total[i]; NaN on div-by-zero or NaN input
    static AlignedSeries as_percent(const AlignedSeries& series, const AlignedSeries& total);

    // Rolling window functions (N-point window; first N-1 points output NaN)
    AlignedSeries rolling_avg(int N) const;     // N-point simple moving average
    AlignedSeries rolling_min(int N) const;     // N-point rolling minimum
    AlignedSeries rolling_max(int N) const;     // N-point rolling maximum
    AlignedSeries rolling_stddev(int N) const;  // N-point rolling population stddev (ddof=0)
    AlignedSeries zscore(int N) const;          // N-point rolling z-score: (v - mean) / stddev; 0 if stddev==0

    // Exponential moving average
    // param <= 1.0: treated as alpha directly; param > 1.0: treated as span N, alpha = 2/(N+1)
    // Leading NaN inputs remain NaN; internal NaN inputs carry forward the last EMA value
    AlignedSeries ema(double param) const;

    // Double exponential smoothing (Holt's linear method)
    // alpha in (0,1]: level smoothing factor
    // beta  in (0,1]: trend smoothing factor
    // Leading NaN inputs remain NaN; NaN inputs inside the series carry forward level/trend
    AlignedSeries holt_winters(double alpha, double beta) const;

    // Timestamp shift: add offsetNs (may be negative) to every timestamp
    // Values are unchanged; timestamps are shifted by exactly offsetNs nanoseconds.
    AlignedSeries time_shift(int64_t offsetNs) const;
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

    AlignedSeries evaluateNode(const ExpressionNode& node, const QueryResultMap& queryResults);

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
