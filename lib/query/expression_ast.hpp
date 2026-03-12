#ifndef EXPRESSION_AST_H_INCLUDED
#define EXPRESSION_AST_H_INCLUDED

#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

namespace timestar {

// Forward declarations
struct ExpressionNode;

// Exception for expression parsing errors
class ExpressionParseException : public std::runtime_error {
public:
    explicit ExpressionParseException(const std::string& message) : std::runtime_error(message) {}
};

// Expression node types
enum class ExprNodeType {
    QUERY_REF,           // Reference to query "a", "b", etc.
    SCALAR,              // Numeric constant: 100, 2.5
    BINARY_OP,           // +, -, *, /
    UNARY_OP,            // -a, abs(), log(), sqrt()
    FUNCTION_CALL,       // min(a, b), max(a, b), clamp(a, lo, hi)
    ANOMALY_FUNCTION,    // anomalies(query, 'algorithm', bounds, 'seasonality')
    FORECAST_FUNCTION,   // forecast(query, 'algorithm', deviations)
    TIME_SHIFT_FUNCTION  // time_shift(query, 'offset') — shift all timestamps by offset
};

// Binary operator types
enum class BinaryOpType {
    ADD,       // +
    SUBTRACT,  // -
    MULTIPLY,  // *
    DIVIDE     // /
};

// Unary operator types
enum class UnaryOpType {
    NEGATE,  // -a
    ABS,     // abs(a)
    LOG,     // log(a) - natural log
    LOG10,   // log10(a)
    SQRT,    // sqrt(a)
    CEIL,    // ceil(a)
    FLOOR,   // floor(a)
    // Transform functions (SIMD-optimized)
    DIFF,            // diff(a) - difference between consecutive points
    MONOTONIC_DIFF,  // monotonic_diff(a) - diff with counter reset handling
    DEFAULT_ZERO,    // default_zero(a) - replace NaN with 0
    COUNT_NONZERO,   // count_nonzero(a) - count of non-zero values (returns scalar)
    COUNT_NOT_NULL,  // count_not_null(a) - count of non-NaN values (returns scalar)
    // Counter-rate functions (use timestamps alongside values)
    RATE,      // rate(a) - per-second rate from monotonically-increasing counter; handles resets
    IRATE,     // irate(a) - instantaneous rate using only the last two points
    INCREASE,  // increase(a) - total increase over the series (sum of positive diffs)
    // Gap-fill / interpolation functions
    FILL_FORWARD,   // fill_forward(a) - last observation carried forward (LOCF); leading NaNs stay NaN
    FILL_BACKWARD,  // fill_backward(a) - next observation carried backward (NOCB); trailing NaNs stay NaN
    FILL_LINEAR,    // fill_linear(a) - linear interpolation between known values; leading/trailing NaN runs stay NaN
    // Accumulation functions
    CUMSUM,    // cumsum(a) - running cumulative sum; NaN treated as 0 (skip-NaN)
    INTEGRAL,  // integral(a) - definite integral using trapezoidal rule; NaN trapezoids contribute 0
    // Normalization
    NORMALIZE  // normalize(a) - rescale values to [0, 1]; constant series → all 0.0; NaN passthrough
};

// Function types for multi-argument functions
enum class FunctionType {
    MIN,    // min(a, b)
    MAX,    // max(a, b)
    CLAMP,  // clamp(value, min, max)
    POW,    // pow(base, exponent)
    // Transform functions with scalar argument (SIMD-optimized)
    CLAMP_MIN,   // clamp_min(series, minVal) - clamp values to minimum
    CLAMP_MAX,   // clamp_max(series, maxVal) - clamp values to maximum
    CUTOFF_MIN,  // cutoff_min(series, threshold) - set values below threshold to NaN
    CUTOFF_MAX,  // cutoff_max(series, threshold) - set values above threshold to NaN
    PER_MINUTE,  // per_minute(series, seconds_per_point) - rate * 60
    PER_HOUR,    // per_hour(series, seconds_per_point) - rate * 3600
    // Rolling window functions (second arg is integer window size N)
    ROLLING_AVG,     // rolling_avg(series, N) - N-point simple moving average
    ROLLING_MIN,     // rolling_min(series, N) - N-point rolling minimum
    ROLLING_MAX,     // rolling_max(series, N) - N-point rolling maximum
    ROLLING_STDDEV,  // rolling_stddev(series, N) - N-point rolling population stddev
    // Gap-fill with constant scalar
    FILL_VALUE,  // fill_value(series, v) - replace every NaN with constant v
    // Exponential moving average
    EMA,  // ema(series, alpha_or_span) - exponential moving average
    // Double exponential smoothing (Holt's linear method)
    HOLT_WINTERS,  // holt_winters(series, alpha, beta) - trend-aware exponential smoother
    // Rolling z-score normalization
    ZSCORE,  // zscore(series, N) - rolling z-score: (value - rolling_mean) / rolling_stddev
    // Cross-series ranking / filtering (operate on a collection of GroupedSeries)
    TOPK,        // topk(N, series) - keep top-N groups by mean value
    BOTTOMK,     // bottomk(N, series) - keep bottom-N groups by mean value
    AS_PERCENT,  // as_percent(series, total) - 100 * series / total; NaN on div-by-zero or NaN input
    // Cross-series aggregation (variadic: operate element-wise across N aligned series)
    AVG_OF_SERIES,        // avg_of_series(a, b, ...) - element-wise mean across series args
    SUM_OF_SERIES,        // sum_of_series(a, b, ...) - element-wise sum across series args
    MIN_OF_SERIES,        // min_of_series(a, b, ...) - element-wise minimum across series args
    MAX_OF_SERIES,        // max_of_series(a, b, ...) - element-wise maximum across series args
    PERCENTILE_OF_SERIES  // percentile_of_series(p, a, b, ...) - p-th percentile across series args
};

// Binary operation node
struct BinaryOp {
    BinaryOpType op;
    std::unique_ptr<ExpressionNode> left;
    std::unique_ptr<ExpressionNode> right;

    BinaryOp(BinaryOpType _op, std::unique_ptr<ExpressionNode> _left, std::unique_ptr<ExpressionNode> _right)
        : op(_op), left(std::move(_left)), right(std::move(_right)) {}
};

// Unary operation node
struct UnaryOp {
    UnaryOpType op;
    std::unique_ptr<ExpressionNode> operand;

    UnaryOp(UnaryOpType _op, std::unique_ptr<ExpressionNode> _operand) : op(_op), operand(std::move(_operand)) {}
};

// Function call node (for multi-argument functions)
struct FunctionCall {
    FunctionType func;
    std::vector<std::unique_ptr<ExpressionNode>> args;

    FunctionCall(FunctionType _func, std::vector<std::unique_ptr<ExpressionNode>> _args)
        : func(_func), args(std::move(_args)) {}
};

// Anomaly function node: anomalies(query, 'algorithm', bounds[, 'seasonality'])
// Returns multiple series: raw, upper bound, lower bound, scores
struct AnomalyFunction {
    std::string queryRef;                    // Reference to query name (e.g., "cpu")
    std::string algorithm;                   // "basic", "agile", "robust"
    double bounds;                           // Standard deviations (1-4)
    std::optional<std::string> seasonality;  // "hourly", "daily", "weekly" (optional)

    AnomalyFunction() = default;
    AnomalyFunction(std::string _queryRef, std::string _algorithm, double _bounds,
                    std::optional<std::string> _seasonality = std::nullopt)
        : queryRef(std::move(_queryRef)),
          algorithm(std::move(_algorithm)),
          bounds(_bounds),
          seasonality(std::move(_seasonality)) {}
};

// Forecast function node: forecast(query, 'algorithm', deviations[, seasonality='daily'][, model='default'][,
// history='1w']) Returns multiple series: past, forecast, upper bound, lower bound
struct ForecastFunction {
    std::string queryRef;                    // Reference to query name (e.g., "cpu")
    std::string algorithm;                   // "linear" or "seasonal"
    double deviations;                       // Standard deviations for confidence bounds (1-4)
    std::optional<std::string> seasonality;  // "hourly", "daily", "weekly" (seasonal only)
    std::optional<std::string> model;        // "default", "simple", "reactive" (linear only)
    std::optional<std::string> history;      // "1w", "3d", "12h", etc. (linear only)

    ForecastFunction() = default;
    ForecastFunction(std::string _queryRef, std::string _algorithm, double _deviations,
                     std::optional<std::string> _seasonality = std::nullopt,
                     std::optional<std::string> _model = std::nullopt,
                     std::optional<std::string> _history = std::nullopt)
        : queryRef(std::move(_queryRef)),
          algorithm(std::move(_algorithm)),
          deviations(_deviations),
          seasonality(std::move(_seasonality)),
          model(std::move(_model)),
          history(std::move(_history)) {}
};

// time_shift function node: time_shift(query, 'offset')
// Shifts all timestamps in the series by the given duration offset.
// Positive offset shifts timestamps forward (e.g. '7d' makes last week look like this week).
// Negative offset (prefixed with '-') shifts timestamps backward.
struct TimeShiftFunction {
    std::string queryRef;  // Reference to query name (e.g., "a")
    std::string offset;    // Duration string, optionally prefixed with '-' (e.g., "7d", "-1h")

    TimeShiftFunction() = default;
    TimeShiftFunction(std::string _queryRef, std::string _offset)
        : queryRef(std::move(_queryRef)), offset(std::move(_offset)) {}
};

// Query reference node
struct QueryRef {
    std::string name;  // "a", "b", "c", etc.

    QueryRef() = default;
    explicit QueryRef(std::string _name) : name(std::move(_name)) {}
};

// Scalar value node
struct ScalarValue {
    double value;

    explicit ScalarValue(double _value) : value(_value) {}
};

// Main expression node using variant
struct ExpressionNode {
    ExprNodeType type;
    std::variant<QueryRef, ScalarValue, BinaryOp, UnaryOp, FunctionCall, AnomalyFunction, ForecastFunction,
                 TimeShiftFunction>
        data;

    // Factory methods for cleaner construction
    static std::unique_ptr<ExpressionNode> makeQueryRef(const std::string& name) {
        auto node = std::make_unique<ExpressionNode>();
        node->type = ExprNodeType::QUERY_REF;
        node->data = QueryRef(name);
        return node;
    }

    static std::unique_ptr<ExpressionNode> makeScalar(double value) {
        auto node = std::make_unique<ExpressionNode>();
        node->type = ExprNodeType::SCALAR;
        node->data = ScalarValue(value);
        return node;
    }

    static std::unique_ptr<ExpressionNode> makeBinaryOp(BinaryOpType op, std::unique_ptr<ExpressionNode> left,
                                                        std::unique_ptr<ExpressionNode> right) {
        auto node = std::make_unique<ExpressionNode>();
        node->type = ExprNodeType::BINARY_OP;
        node->data = BinaryOp(op, std::move(left), std::move(right));
        return node;
    }

    static std::unique_ptr<ExpressionNode> makeUnaryOp(UnaryOpType op, std::unique_ptr<ExpressionNode> operand) {
        auto node = std::make_unique<ExpressionNode>();
        node->type = ExprNodeType::UNARY_OP;
        node->data = UnaryOp(op, std::move(operand));
        return node;
    }

    static std::unique_ptr<ExpressionNode> makeFunctionCall(FunctionType func,
                                                            std::vector<std::unique_ptr<ExpressionNode>> args) {
        auto node = std::make_unique<ExpressionNode>();
        node->type = ExprNodeType::FUNCTION_CALL;
        node->data = FunctionCall(func, std::move(args));
        return node;
    }

    static std::unique_ptr<ExpressionNode> makeAnomalyFunction(
        const std::string& queryRef, const std::string& algorithm, double bounds,
        const std::optional<std::string>& seasonality = std::nullopt) {
        auto node = std::make_unique<ExpressionNode>();
        node->type = ExprNodeType::ANOMALY_FUNCTION;
        node->data = AnomalyFunction(queryRef, algorithm, bounds, seasonality);
        return node;
    }

    static std::unique_ptr<ExpressionNode> makeForecastFunction(
        const std::string& queryRef, const std::string& algorithm, double deviations,
        const std::optional<std::string>& seasonality = std::nullopt,
        const std::optional<std::string>& model = std::nullopt,
        const std::optional<std::string>& history = std::nullopt) {
        auto node = std::make_unique<ExpressionNode>();
        node->type = ExprNodeType::FORECAST_FUNCTION;
        node->data = ForecastFunction(queryRef, algorithm, deviations, seasonality, model, history);
        return node;
    }

    static std::unique_ptr<ExpressionNode> makeTimeShiftFunction(const std::string& queryRef,
                                                                 const std::string& offset) {
        auto node = std::make_unique<ExpressionNode>();
        node->type = ExprNodeType::TIME_SHIFT_FUNCTION;
        node->data = TimeShiftFunction(queryRef, offset);
        return node;
    }

    // Accessors with type checking
    const QueryRef& asQueryRef() const {
        if (type != ExprNodeType::QUERY_REF) {
            throw std::runtime_error("Node is not a QueryRef");
        }
        return std::get<QueryRef>(data);
    }

    const ScalarValue& asScalar() const {
        if (type != ExprNodeType::SCALAR) {
            throw std::runtime_error("Node is not a Scalar");
        }
        return std::get<ScalarValue>(data);
    }

    const BinaryOp& asBinaryOp() const {
        if (type != ExprNodeType::BINARY_OP) {
            throw std::runtime_error("Node is not a BinaryOp");
        }
        return std::get<BinaryOp>(data);
    }

    const UnaryOp& asUnaryOp() const {
        if (type != ExprNodeType::UNARY_OP) {
            throw std::runtime_error("Node is not a UnaryOp");
        }
        return std::get<UnaryOp>(data);
    }

    const FunctionCall& asFunctionCall() const {
        if (type != ExprNodeType::FUNCTION_CALL) {
            throw std::runtime_error("Node is not a FunctionCall");
        }
        return std::get<FunctionCall>(data);
    }

    const AnomalyFunction& asAnomalyFunction() const {
        if (type != ExprNodeType::ANOMALY_FUNCTION) {
            throw std::runtime_error("Node is not an AnomalyFunction");
        }
        return std::get<AnomalyFunction>(data);
    }

    const ForecastFunction& asForecastFunction() const {
        if (type != ExprNodeType::FORECAST_FUNCTION) {
            throw std::runtime_error("Node is not a ForecastFunction");
        }
        return std::get<ForecastFunction>(data);
    }

    const TimeShiftFunction& asTimeShiftFunction() const {
        if (type != ExprNodeType::TIME_SHIFT_FUNCTION) {
            throw std::runtime_error("Node is not a TimeShiftFunction");
        }
        return std::get<TimeShiftFunction>(data);
    }

    // Convert expression tree to string (for debugging/testing)
    std::string toString() const;
};

// Helper to convert operator types to strings
inline const char* binaryOpToString(BinaryOpType op) {
    switch (op) {
        case BinaryOpType::ADD:
            return "+";
        case BinaryOpType::SUBTRACT:
            return "-";
        case BinaryOpType::MULTIPLY:
            return "*";
        case BinaryOpType::DIVIDE:
            return "/";
        default:
            return "?";
    }
}

inline const char* unaryOpToString(UnaryOpType op) {
    switch (op) {
        case UnaryOpType::NEGATE:
            return "-";
        case UnaryOpType::ABS:
            return "abs";
        case UnaryOpType::LOG:
            return "log";
        case UnaryOpType::LOG10:
            return "log10";
        case UnaryOpType::SQRT:
            return "sqrt";
        case UnaryOpType::CEIL:
            return "ceil";
        case UnaryOpType::FLOOR:
            return "floor";
        case UnaryOpType::DIFF:
            return "diff";
        case UnaryOpType::MONOTONIC_DIFF:
            return "monotonic_diff";
        case UnaryOpType::DEFAULT_ZERO:
            return "default_zero";
        case UnaryOpType::COUNT_NONZERO:
            return "count_nonzero";
        case UnaryOpType::COUNT_NOT_NULL:
            return "count_not_null";
        case UnaryOpType::RATE:
            return "rate";
        case UnaryOpType::IRATE:
            return "irate";
        case UnaryOpType::INCREASE:
            return "increase";
        case UnaryOpType::FILL_FORWARD:
            return "fill_forward";
        case UnaryOpType::FILL_BACKWARD:
            return "fill_backward";
        case UnaryOpType::FILL_LINEAR:
            return "fill_linear";
        case UnaryOpType::CUMSUM:
            return "cumsum";
        case UnaryOpType::INTEGRAL:
            return "integral";
        case UnaryOpType::NORMALIZE:
            return "normalize";
        default:
            return "?";
    }
}

inline const char* functionToString(FunctionType func) {
    switch (func) {
        case FunctionType::MIN:
            return "min";
        case FunctionType::MAX:
            return "max";
        case FunctionType::CLAMP:
            return "clamp";
        case FunctionType::POW:
            return "pow";
        case FunctionType::CLAMP_MIN:
            return "clamp_min";
        case FunctionType::CLAMP_MAX:
            return "clamp_max";
        case FunctionType::CUTOFF_MIN:
            return "cutoff_min";
        case FunctionType::CUTOFF_MAX:
            return "cutoff_max";
        case FunctionType::PER_MINUTE:
            return "per_minute";
        case FunctionType::PER_HOUR:
            return "per_hour";
        case FunctionType::ROLLING_AVG:
            return "rolling_avg";
        case FunctionType::ROLLING_MIN:
            return "rolling_min";
        case FunctionType::ROLLING_MAX:
            return "rolling_max";
        case FunctionType::ROLLING_STDDEV:
            return "rolling_stddev";
        case FunctionType::FILL_VALUE:
            return "fill_value";
        case FunctionType::EMA:
            return "ema";
        case FunctionType::HOLT_WINTERS:
            return "holt_winters";
        case FunctionType::ZSCORE:
            return "zscore";
        case FunctionType::TOPK:
            return "topk";
        case FunctionType::BOTTOMK:
            return "bottomk";
        case FunctionType::AS_PERCENT:
            return "as_percent";
        case FunctionType::AVG_OF_SERIES:
            return "avg_of_series";
        case FunctionType::SUM_OF_SERIES:
            return "sum_of_series";
        case FunctionType::MIN_OF_SERIES:
            return "min_of_series";
        case FunctionType::MAX_OF_SERIES:
            return "max_of_series";
        case FunctionType::PERCENTILE_OF_SERIES:
            return "percentile_of_series";
        default:
            return "?";
    }
}

}  // namespace timestar

#endif  // EXPRESSION_AST_H_INCLUDED
