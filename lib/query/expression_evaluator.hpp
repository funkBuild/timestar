#ifndef EXPRESSION_EVALUATOR_H_INCLUDED
#define EXPRESSION_EVALUATOR_H_INCLUDED

#include "expression_ast.hpp"
#include "derived_query.hpp"
#include <map>
#include <string>
#include <vector>
#include <cmath>
#include <algorithm>
#include <stdexcept>

namespace tsdb {

// Exception for evaluation errors
class EvaluationException : public std::runtime_error {
public:
    explicit EvaluationException(const std::string& message)
        : std::runtime_error(message) {}
};

// A time series that has been aligned (same timestamps across all series)
struct AlignedSeries {
    std::vector<uint64_t> timestamps;
    std::vector<double> values;

    AlignedSeries() = default;

    AlignedSeries(std::vector<uint64_t> ts, std::vector<double> vals)
        : timestamps(std::move(ts)), values(std::move(vals)) {
        if (timestamps.size() != values.size()) {
            throw EvaluationException(
                "Timestamp and value vectors must have the same size");
        }
    }

    // Move constructor and assignment (defaulted, but explicit for clarity)
    AlignedSeries(AlignedSeries&&) = default;
    AlignedSeries& operator=(AlignedSeries&&) = default;
    AlignedSeries(const AlignedSeries&) = default;
    AlignedSeries& operator=(const AlignedSeries&) = default;

    bool empty() const { return timestamps.empty(); }
    size_t size() const { return timestamps.size(); }

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
    AlignedSeries diff() const;             // Difference between consecutive points
    AlignedSeries monotonic_diff() const;   // Diff with counter reset handling
    AlignedSeries default_zero() const;     // Replace NaN with 0
    AlignedSeries count_nonzero() const;    // Count non-zero values (returns scalar series)
    AlignedSeries count_not_null() const;   // Count non-NaN values (returns scalar series)

    // Binary functions
    static AlignedSeries min(const AlignedSeries& a, const AlignedSeries& b);
    static AlignedSeries max(const AlignedSeries& a, const AlignedSeries& b);
    static AlignedSeries pow(const AlignedSeries& base, const AlignedSeries& exp);
    static AlignedSeries clamp(const AlignedSeries& val,
                               const AlignedSeries& minVal,
                               const AlignedSeries& maxVal);

    // Transform functions with scalar argument
    AlignedSeries clamp_min(double minVal) const;   // Clamp values to minimum
    AlignedSeries clamp_max(double maxVal) const;   // Clamp values to maximum
    AlignedSeries cutoff_min(double threshold) const; // Set values below threshold to NaN
    AlignedSeries cutoff_max(double threshold) const; // Set values above threshold to NaN
    AlignedSeries per_minute(double seconds_per_point) const; // Rate * 60
    AlignedSeries per_hour(double seconds_per_point) const;   // Rate * 3600
};

// Evaluates expression ASTs against aligned time series data
class ExpressionEvaluator {
public:
    // Map of query name to aligned series data
    using QueryResultMap = std::map<std::string, AlignedSeries>;

    ExpressionEvaluator() = default;

    // Evaluate an expression against the provided query results
    // All series in queryResults must already be aligned (same timestamps)
    AlignedSeries evaluate(const ExpressionNode& expr,
                          const QueryResultMap& queryResults);

private:
    AlignedSeries evaluateNode(const ExpressionNode& node,
                               const QueryResultMap& queryResults);

    const AlignedSeries& evaluateQueryRef(const QueryRef& ref,
                                         const QueryResultMap& queryResults);

    AlignedSeries evaluateScalar(const ScalarValue& scalar,
                                 const QueryResultMap& queryResults);

    AlignedSeries evaluateBinaryOp(const BinaryOp& op,
                                   const QueryResultMap& queryResults);

    AlignedSeries evaluateUnaryOp(const UnaryOp& op,
                                  const QueryResultMap& queryResults);

    AlignedSeries evaluateFunctionCall(const FunctionCall& call,
                                       const QueryResultMap& queryResults);

    // Create a scalar series (same value at all timestamps)
    AlignedSeries makeScalarSeries(double value, size_t size,
                                   const std::vector<uint64_t>& timestamps) const;

    // Get the reference timestamps from query results
    const std::vector<uint64_t>& getReferenceTimestamps(
        const QueryResultMap& queryResults) const;
};

} // namespace tsdb

#endif // EXPRESSION_EVALUATOR_H_INCLUDED
