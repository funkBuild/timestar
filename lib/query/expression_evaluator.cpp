#include "expression_evaluator.hpp"
#include <limits>
#include <cmath>

namespace tsdb {

// ==================== AlignedSeries Operations ====================

namespace {
    void checkSameSize(const AlignedSeries& a, const AlignedSeries& b,
                       const std::string& operation) {
        if (a.size() != b.size()) {
            throw EvaluationException(
                "Series size mismatch in " + operation + ": " +
                std::to_string(a.size()) + " vs " + std::to_string(b.size()));
        }
    }
}

AlignedSeries AlignedSeries::operator+(const AlignedSeries& other) const {
    checkSameSize(*this, other, "addition");
    // TODO: timestamps are copied here; consider sharing via shared_ptr to avoid allocation
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = values[i] + other.values[i];
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::operator-(const AlignedSeries& other) const {
    checkSameSize(*this, other, "subtraction");
    // TODO: timestamps are copied here; consider sharing via shared_ptr to avoid allocation
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = values[i] - other.values[i];
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::operator*(const AlignedSeries& other) const {
    checkSameSize(*this, other, "multiplication");
    // TODO: timestamps are copied here; consider sharing via shared_ptr to avoid allocation
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = values[i] * other.values[i];
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::operator/(const AlignedSeries& other) const {
    checkSameSize(*this, other, "division");
    // TODO: timestamps are copied here; consider sharing via shared_ptr to avoid allocation
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        if (other.values[i] == 0.0) {
            // Handle division by zero: result is NaN or Inf depending on numerator
            if (values[i] == 0.0) {
                result[i] = std::numeric_limits<double>::quiet_NaN();
            } else {
                result[i] = values[i] > 0
                    ? std::numeric_limits<double>::infinity()
                    : -std::numeric_limits<double>::infinity();
            }
        } else {
            result[i] = values[i] / other.values[i];
        }
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::operator+(double scalar) const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = values[i] + scalar;
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::operator-(double scalar) const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = values[i] - scalar;
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::operator*(double scalar) const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = values[i] * scalar;
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::operator/(double scalar) const {
    if (scalar == 0.0) {
        throw EvaluationException("Division by zero scalar");
    }
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = values[i] / scalar;
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::negate() const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = -values[i];
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::abs() const {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = std::abs(values[i]);
    }
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

AlignedSeries AlignedSeries::clamp(const AlignedSeries& val,
                                   const AlignedSeries& minVal,
                                   const AlignedSeries& maxVal) {
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
    result[0] = std::numeric_limits<double>::quiet_NaN(); // First point has no previous
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
    result[0] = std::numeric_limits<double>::quiet_NaN(); // First point has no previous
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

AlignedSeries AlignedSeries::per_minute(double seconds_per_point) const {
    if (values.empty()) {
        return AlignedSeries(timestamps, {});
    }

    std::vector<double> result(values.size());
    result[0] = std::numeric_limits<double>::quiet_NaN(); // First point has no previous

    for (size_t i = 1; i < values.size(); ++i) {
        double value_diff = values[i] - values[i - 1];

        // Compute time delta in seconds from timestamps (nanoseconds)
        double dt_seconds;
        if (timestamps[i] <= timestamps[i - 1]) {
            // Zero or negative time delta: output 0.0 to avoid division by zero
            result[i] = 0.0;
            continue;
        }
        dt_seconds = static_cast<double>(timestamps[i] - timestamps[i - 1]) / 1e9;

        // Fall back to the provided seconds_per_point if timestamps yield zero
        if (dt_seconds == 0.0) {
            if (seconds_per_point == 0.0) {
                result[i] = 0.0;
                continue;
            }
            dt_seconds = seconds_per_point;
        }

        // Rate per minute: (value_diff / dt_seconds) * 60
        result[i] = (value_diff / dt_seconds) * 60.0;
    }
    return AlignedSeries(timestamps, std::move(result));
}

AlignedSeries AlignedSeries::per_hour(double seconds_per_point) const {
    if (values.empty()) {
        return AlignedSeries(timestamps, {});
    }

    std::vector<double> result(values.size());
    result[0] = std::numeric_limits<double>::quiet_NaN(); // First point has no previous

    for (size_t i = 1; i < values.size(); ++i) {
        double value_diff = values[i] - values[i - 1];

        // Compute time delta in seconds from timestamps (nanoseconds)
        double dt_seconds;
        if (timestamps[i] <= timestamps[i - 1]) {
            // Zero or negative time delta: output 0.0 to avoid division by zero
            result[i] = 0.0;
            continue;
        }
        dt_seconds = static_cast<double>(timestamps[i] - timestamps[i - 1]) / 1e9;

        // Fall back to the provided seconds_per_point if timestamps yield zero
        if (dt_seconds == 0.0) {
            if (seconds_per_point == 0.0) {
                result[i] = 0.0;
                continue;
            }
            dt_seconds = seconds_per_point;
        }

        // Rate per hour: (value_diff / dt_seconds) * 3600
        result[i] = (value_diff / dt_seconds) * 3600.0;
    }
    return AlignedSeries(timestamps, std::move(result));
}

// ==================== ExpressionEvaluator ====================

AlignedSeries ExpressionEvaluator::evaluate(const ExpressionNode& expr,
                                            const QueryResultMap& queryResults) {
    if (queryResults.empty()) {
        throw EvaluationException("No query results provided for evaluation");
    }

    return evaluateNode(expr, queryResults);
}

AlignedSeries ExpressionEvaluator::evaluateNode(const ExpressionNode& node,
                                                 const QueryResultMap& queryResults) {
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

        default:
            throw EvaluationException("Unknown expression node type");
    }
}

const AlignedSeries& ExpressionEvaluator::evaluateQueryRef(const QueryRef& ref,
                                                           const QueryResultMap& queryResults) {
    auto it = queryResults.find(ref.name);
    if (it == queryResults.end()) {
        throw EvaluationException("Query '" + ref.name + "' not found in results");
    }
    return it->second;
}

AlignedSeries ExpressionEvaluator::evaluateScalar(const ScalarValue& scalar,
                                                   const QueryResultMap& queryResults) {
    const auto& timestamps = getReferenceTimestamps(queryResults);
    return makeScalarSeries(scalar.value, timestamps.size(), timestamps);
}

AlignedSeries ExpressionEvaluator::evaluateBinaryOp(const BinaryOp& op,
                                                     const QueryResultMap& queryResults) {
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

AlignedSeries ExpressionEvaluator::evaluateUnaryOp(const UnaryOp& op,
                                                    const QueryResultMap& queryResults) {
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
        default:
            throw EvaluationException("Unknown unary operator");
    }
}

AlignedSeries ExpressionEvaluator::evaluateFunctionCall(const FunctionCall& call,
                                                         const QueryResultMap& queryResults) {
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

        default:
            throw EvaluationException("Unknown function type");
    }
}

AlignedSeries ExpressionEvaluator::makeScalarSeries(double value, size_t size,
                                                     const std::vector<uint64_t>& timestamps) const {
    std::vector<double> values(size, value);
    return AlignedSeries(timestamps, std::move(values));
}

const std::vector<uint64_t>& ExpressionEvaluator::getReferenceTimestamps(
    const QueryResultMap& queryResults) const {
    // Use the first non-empty series timestamps as reference
    for (const auto& [name, series] : queryResults) {
        if (!series.empty()) {
            return series.timestamps;
        }
    }
    throw EvaluationException("No non-empty series found in query results");
}

} // namespace tsdb
