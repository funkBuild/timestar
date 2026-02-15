#include "expression_parser.hpp"
#include <cctype>
#include <sstream>
#include <algorithm>
#include <stdexcept>

namespace tsdb {

// ==================== ExpressionNode::toString ====================

std::string ExpressionNode::toString() const {
    switch (type) {
        case ExprNodeType::QUERY_REF:
            return asQueryRef().name;

        case ExprNodeType::SCALAR: {
            std::ostringstream oss;
            oss << asScalar().value;
            return oss.str();
        }

        case ExprNodeType::BINARY_OP: {
            const auto& op = asBinaryOp();
            return "(" + op.left->toString() + " " +
                   binaryOpToString(op.op) + " " +
                   op.right->toString() + ")";
        }

        case ExprNodeType::UNARY_OP: {
            const auto& op = asUnaryOp();
            if (op.op == UnaryOpType::NEGATE) {
                return "(-" + op.operand->toString() + ")";
            }
            return std::string(unaryOpToString(op.op)) + "(" +
                   op.operand->toString() + ")";
        }

        case ExprNodeType::FUNCTION_CALL: {
            const auto& call = asFunctionCall();
            std::string result = std::string(functionToString(call.func)) + "(";
            for (size_t i = 0; i < call.args.size(); ++i) {
                if (i > 0) result += ", ";
                result += call.args[i]->toString();
            }
            result += ")";
            return result;
        }

        case ExprNodeType::ANOMALY_FUNCTION: {
            const auto& anomaly = asAnomalyFunction();
            std::string result = "anomalies(" + anomaly.queryRef + ", '" +
                                 anomaly.algorithm + "', " +
                                 std::to_string(anomaly.bounds);
            if (anomaly.seasonality.has_value()) {
                result += ", '" + anomaly.seasonality.value() + "'";
            }
            result += ")";
            return result;
        }

        case ExprNodeType::FORECAST_FUNCTION: {
            const auto& forecast = asForecastFunction();
            std::string result = "forecast(" + forecast.queryRef + ", '" +
                                 forecast.algorithm + "', " +
                                 std::to_string(forecast.deviations);
            if (forecast.seasonality.has_value()) {
                result += ", seasonality='" + forecast.seasonality.value() + "'";
            }
            if (forecast.model.has_value()) {
                result += ", model='" + forecast.model.value() + "'";
            }
            if (forecast.history.has_value()) {
                result += ", history='" + forecast.history.value() + "'";
            }
            result += ")";
            return result;
        }
    }
    return "?";
}

// ==================== ExpressionLexer ====================

ExpressionLexer::ExpressionLexer(std::string_view input)
    : input_(input) {}

void ExpressionLexer::skipWhitespace() {
    while (!isAtEnd() && std::isspace(peek())) {
        advance();
    }
}

char ExpressionLexer::peek() const {
    if (isAtEnd()) return '\0';
    return input_[pos_];
}

char ExpressionLexer::advance() {
    return input_[pos_++];
}

bool ExpressionLexer::isAtEnd() const {
    return pos_ >= input_.size();
}

Token ExpressionLexer::readNumber() {
    size_t start = pos_;
    size_t startPos = pos_;

    // Integer part
    while (!isAtEnd() && std::isdigit(peek())) {
        advance();
    }

    // Decimal part
    if (!isAtEnd() && peek() == '.' && pos_ + 1 < input_.size() &&
        std::isdigit(input_[pos_ + 1])) {
        advance(); // consume '.'
        while (!isAtEnd() && std::isdigit(peek())) {
            advance();
        }
    }

    // Scientific notation
    if (!isAtEnd() && (peek() == 'e' || peek() == 'E')) {
        size_t ePos = pos_;
        advance();
        if (!isAtEnd() && (peek() == '+' || peek() == '-')) {
            advance();
        }
        if (!isAtEnd() && std::isdigit(peek())) {
            while (!isAtEnd() && std::isdigit(peek())) {
                advance();
            }
        } else {
            // Not valid scientific notation, backtrack
            pos_ = ePos;
        }
    }

    return Token(TokenType::NUMBER,
                 std::string(input_.substr(start, pos_ - start)),
                 startPos);
}

Token ExpressionLexer::readIdentifier() {
    size_t start = pos_;
    size_t startPos = pos_;

    // First character already validated as alpha or underscore
    advance();

    // Continue with alphanumeric or underscore
    while (!isAtEnd() && (std::isalnum(peek()) || peek() == '_')) {
        advance();
    }

    return Token(TokenType::IDENTIFIER,
                 std::string(input_.substr(start, pos_ - start)),
                 startPos);
}

Token ExpressionLexer::readString() {
    size_t startPos = pos_;
    advance(); // consume opening quote

    size_t start = pos_;
    while (!isAtEnd() && peek() != '\'') {
        advance();
    }

    if (isAtEnd()) {
        throw ExpressionParseException(
            "Unterminated string literal at position " + std::to_string(startPos));
    }

    std::string value(input_.substr(start, pos_ - start));
    advance(); // consume closing quote

    return Token(TokenType::STRING, value, startPos);
}

Token ExpressionLexer::peekToken() {
    if (peeked_.has_value()) {
        return peeked_.value();
    }
    peeked_ = nextToken();
    return peeked_.value();
}

Token ExpressionLexer::nextToken() {
    if (peeked_.has_value()) {
        Token t = std::move(peeked_.value());
        peeked_.reset();
        return t;
    }

    skipWhitespace();

    if (isAtEnd()) {
        return Token(TokenType::END, "", pos_);
    }

    size_t startPos = pos_;
    char c = peek();

    // Single character tokens
    switch (c) {
        case '+': advance(); return Token(TokenType::PLUS, "+", startPos);
        case '-': advance(); return Token(TokenType::MINUS, "-", startPos);
        case '*': advance(); return Token(TokenType::STAR, "*", startPos);
        case '/': advance(); return Token(TokenType::SLASH, "/", startPos);
        case '(': advance(); return Token(TokenType::LPAREN, "(", startPos);
        case ')': advance(); return Token(TokenType::RPAREN, ")", startPos);
        case ',': advance(); return Token(TokenType::COMMA, ",", startPos);
        case '=': advance(); return Token(TokenType::EQUALS, "=", startPos);
        default: break;
    }

    // Numbers
    if (std::isdigit(c)) {
        return readNumber();
    }

    // String literals (single-quoted)
    if (c == '\'') {
        return readString();
    }

    // Identifiers (query refs and function names)
    if (std::isalpha(c) || c == '_') {
        return readIdentifier();
    }

    throw ExpressionParseException(
        "Unexpected character '" + std::string(1, c) +
        "' at position " + std::to_string(pos_));
}

// ==================== ExpressionParser ====================

ExpressionParser::ExpressionParser(std::string_view input)
    : lexer_(input), currentToken_(TokenType::END, "", 0) {
    advance(); // Load first token
}

void ExpressionParser::advance() {
    currentToken_ = lexer_.nextToken();
}

bool ExpressionParser::check(TokenType type) const {
    return currentToken_.type == type;
}

bool ExpressionParser::match(TokenType type) {
    if (check(type)) {
        advance();
        return true;
    }
    return false;
}

void ExpressionParser::consume(TokenType type, const std::string& message) {
    if (!check(type)) {
        error(message);
    }
    advance();
}

void ExpressionParser::error(const std::string& message) const {
    throw ExpressionParseException(
        message + " at position " + std::to_string(currentToken_.position) +
        " (got '" + currentToken_.value + "')");
}

int ExpressionParser::getPrecedence() const {
    switch (currentToken_.type) {
        case TokenType::PLUS:
        case TokenType::MINUS:
            return PREC_TERM;
        case TokenType::STAR:
        case TokenType::SLASH:
            return PREC_FACTOR;
        default:
            return PREC_NONE;
    }
}

std::optional<BinaryOpType> ExpressionParser::getBinaryOp() const {
    switch (currentToken_.type) {
        case TokenType::PLUS: return BinaryOpType::ADD;
        case TokenType::MINUS: return BinaryOpType::SUBTRACT;
        case TokenType::STAR: return BinaryOpType::MULTIPLY;
        case TokenType::SLASH: return BinaryOpType::DIVIDE;
        default: return std::nullopt;
    }
}

bool ExpressionParser::isFunction(const std::string& name) const {
    static const std::set<std::string> functions = {
        "abs", "log", "log10", "sqrt", "ceil", "floor",  // unary math
        "diff", "monotonic_diff", "default_zero",         // unary transform
        "count_nonzero", "count_not_null",               // unary aggregating
        "min", "max", "pow", "clamp",                     // multi-arg math
        "clamp_min", "clamp_max",                         // clamp with scalar
        "cutoff_min", "cutoff_max",                       // cutoff with scalar
        "per_minute", "per_hour",                         // rate conversion
        "anomalies",                                      // anomaly detection
        "forecast"                                        // forecasting
    };
    return functions.count(name) > 0;
}

bool ExpressionParser::isAnomalyFunction(const std::string& name) const {
    return name == "anomalies";
}

bool ExpressionParser::isForecastFunction(const std::string& name) const {
    return name == "forecast";
}

std::optional<UnaryOpType> ExpressionParser::getUnaryFunction(const std::string& name) const {
    // Math functions
    if (name == "abs") return UnaryOpType::ABS;
    if (name == "log") return UnaryOpType::LOG;
    if (name == "log10") return UnaryOpType::LOG10;
    if (name == "sqrt") return UnaryOpType::SQRT;
    if (name == "ceil") return UnaryOpType::CEIL;
    if (name == "floor") return UnaryOpType::FLOOR;
    // Transform functions (SIMD-optimized)
    if (name == "diff") return UnaryOpType::DIFF;
    if (name == "monotonic_diff") return UnaryOpType::MONOTONIC_DIFF;
    if (name == "default_zero") return UnaryOpType::DEFAULT_ZERO;
    if (name == "count_nonzero") return UnaryOpType::COUNT_NONZERO;
    if (name == "count_not_null") return UnaryOpType::COUNT_NOT_NULL;
    return std::nullopt;
}

std::optional<FunctionType> ExpressionParser::getMultiArgFunction(const std::string& name) const {
    // Math functions
    if (name == "min") return FunctionType::MIN;
    if (name == "max") return FunctionType::MAX;
    if (name == "pow") return FunctionType::POW;
    if (name == "clamp") return FunctionType::CLAMP;
    // Transform functions with scalar argument (SIMD-optimized)
    if (name == "clamp_min") return FunctionType::CLAMP_MIN;
    if (name == "clamp_max") return FunctionType::CLAMP_MAX;
    if (name == "cutoff_min") return FunctionType::CUTOFF_MIN;
    if (name == "cutoff_max") return FunctionType::CUTOFF_MAX;
    if (name == "per_minute") return FunctionType::PER_MINUTE;
    if (name == "per_hour") return FunctionType::PER_HOUR;
    return std::nullopt;
}

std::unique_ptr<ExpressionNode> ExpressionParser::parse() {
    auto expr = parseExpression();

    if (!check(TokenType::END)) {
        error("Expected end of expression");
    }

    return expr;
}

std::unique_ptr<ExpressionNode> ExpressionParser::parseExpression(int precedence) {
    auto left = parseUnary();

    while (getPrecedence() > precedence) {
        auto opOpt = getBinaryOp();
        if (!opOpt.has_value()) {
            break;
        }

        BinaryOpType op = opOpt.value();
        int opPrec = getPrecedence();
        advance(); // consume operator

        auto right = parseExpression(opPrec);
        left = ExpressionNode::makeBinaryOp(op, std::move(left), std::move(right));
    }

    return left;
}

std::unique_ptr<ExpressionNode> ExpressionParser::parseUnary() {
    // Handle unary minus
    if (match(TokenType::MINUS)) {
        auto operand = parseUnary();
        return ExpressionNode::makeUnaryOp(UnaryOpType::NEGATE, std::move(operand));
    }

    // Handle unary plus (just ignore it)
    if (match(TokenType::PLUS)) {
        return parseUnary();
    }

    return parsePrimary();
}

std::unique_ptr<ExpressionNode> ExpressionParser::parsePrimary() {
    if (check(TokenType::NUMBER)) {
        return parseNumber();
    }

    if (check(TokenType::IDENTIFIER)) {
        return parseIdentifier();
    }

    if (check(TokenType::LPAREN)) {
        return parseGrouping();
    }

    error("Expected expression");
    return nullptr; // unreachable
}

std::unique_ptr<ExpressionNode> ExpressionParser::parseNumber() {
    double value;
    try {
        value = std::stod(currentToken_.value);
    } catch (const std::out_of_range&) {
        throw ExpressionParseException("Number out of range: " + currentToken_.value);
    } catch (const std::invalid_argument&) {
        throw ExpressionParseException("Invalid number: " + currentToken_.value);
    }
    advance();
    return ExpressionNode::makeScalar(value);
}

std::unique_ptr<ExpressionNode> ExpressionParser::parseIdentifier() {
    std::string name = currentToken_.value;
    advance();

    // Check if it's a function call
    if (check(TokenType::LPAREN)) {
        return parseFunctionCall(name);
    }

    // Otherwise it's a query reference
    queryRefs_.insert(name);
    return ExpressionNode::makeQueryRef(name);
}

std::unique_ptr<ExpressionNode> ExpressionParser::parseAnomalyFunction() {
    consume(TokenType::LPAREN, "Expected '(' after 'anomalies'");

    // First argument: query reference (identifier)
    if (!check(TokenType::IDENTIFIER)) {
        error("Expected query reference as first argument to anomalies()");
    }
    std::string queryRef = currentToken_.value;
    queryRefs_.insert(queryRef);
    advance();

    consume(TokenType::COMMA, "Expected ',' after query reference");

    // Second argument: algorithm (string literal)
    if (!check(TokenType::STRING)) {
        error("Expected algorithm string ('basic', 'agile', or 'robust') as second argument");
    }
    std::string algorithm = currentToken_.value;
    if (algorithm != "basic" && algorithm != "agile" && algorithm != "robust") {
        throw ExpressionParseException(
            "Invalid algorithm '" + algorithm + "'. Use 'basic', 'agile', or 'robust'");
    }
    advance();

    consume(TokenType::COMMA, "Expected ',' after algorithm");

    // Third argument: bounds (number)
    if (!check(TokenType::NUMBER)) {
        error("Expected bounds number (1-4) as third argument");
    }
    double bounds;
    try {
        bounds = std::stod(currentToken_.value);
    } catch (const std::out_of_range&) {
        throw ExpressionParseException("Number out of range: " + currentToken_.value);
    } catch (const std::invalid_argument&) {
        throw ExpressionParseException("Invalid number: " + currentToken_.value);
    }
    if (bounds < 1.0 || bounds > 4.0) {
        throw ExpressionParseException(
            "Bounds must be between 1 and 4, got " + std::to_string(bounds));
    }
    advance();

    // Fourth argument (optional): seasonality (string literal)
    std::optional<std::string> seasonality;
    if (match(TokenType::COMMA)) {
        if (!check(TokenType::STRING)) {
            error("Expected seasonality string ('hourly', 'daily', or 'weekly')");
        }
        std::string seasonStr = currentToken_.value;
        if (seasonStr != "hourly" && seasonStr != "daily" && seasonStr != "weekly") {
            throw ExpressionParseException(
                "Invalid seasonality '" + seasonStr + "'. Use 'hourly', 'daily', or 'weekly'");
        }
        seasonality = seasonStr;
        advance();
    }

    consume(TokenType::RPAREN, "Expected ')' after anomalies arguments");

    return ExpressionNode::makeAnomalyFunction(queryRef, algorithm, bounds, seasonality);
}

std::unique_ptr<ExpressionNode> ExpressionParser::parseForecastFunction() {
    consume(TokenType::LPAREN, "Expected '(' after 'forecast'");

    // First argument: query reference (identifier)
    if (!check(TokenType::IDENTIFIER)) {
        error("Expected query reference as first argument to forecast()");
    }
    std::string queryRef = currentToken_.value;
    queryRefs_.insert(queryRef);
    advance();

    consume(TokenType::COMMA, "Expected ',' after query reference");

    // Second argument: algorithm (string literal)
    if (!check(TokenType::STRING)) {
        error("Expected algorithm string ('linear' or 'seasonal') as second argument");
    }
    std::string algorithm = currentToken_.value;
    if (algorithm != "linear" && algorithm != "seasonal") {
        throw ExpressionParseException(
            "Invalid forecast algorithm '" + algorithm + "'. Use 'linear' or 'seasonal'");
    }
    advance();

    consume(TokenType::COMMA, "Expected ',' after algorithm");

    // Third argument: deviations (number)
    if (!check(TokenType::NUMBER)) {
        error("Expected deviations number (1-4) as third argument");
    }
    double deviations;
    try {
        deviations = std::stod(currentToken_.value);
    } catch (const std::out_of_range&) {
        throw ExpressionParseException("Number out of range: " + currentToken_.value);
    } catch (const std::invalid_argument&) {
        throw ExpressionParseException("Invalid number: " + currentToken_.value);
    }
    if (deviations < 1.0 || deviations > 4.0) {
        throw ExpressionParseException(
            "Deviations must be between 1 and 4, got " + std::to_string(deviations));
    }
    advance();

    // Optional arguments: seasonality='<string>', model='<string>', history='<string>'
    std::optional<std::string> seasonality;
    std::optional<std::string> model;
    std::optional<std::string> history;

    while (match(TokenType::COMMA)) {
        // Expect parameter name identifier
        if (!check(TokenType::IDENTIFIER)) {
            error("Expected parameter name (seasonality, model, or history)");
        }
        std::string paramName = currentToken_.value;
        advance();

        // Expect '=' token
        consume(TokenType::EQUALS, "Expected '=' after parameter name");

        // Expect string value
        if (!check(TokenType::STRING)) {
            error("Expected string value for parameter '" + paramName + "'");
        }
        std::string paramValue = currentToken_.value;
        advance();

        // Parse based on parameter name
        if (paramName == "seasonality") {
            if (paramValue != "hourly" && paramValue != "daily" &&
                paramValue != "weekly" && paramValue != "auto" && paramValue != "multi") {
                throw ExpressionParseException(
                    "Invalid seasonality '" + paramValue + "'. Use 'hourly', 'daily', 'weekly', 'auto', or 'multi'");
            }
            seasonality = paramValue;
        } else if (paramName == "model") {
            if (paramValue != "default" && paramValue != "simple" && paramValue != "reactive") {
                throw ExpressionParseException(
                    "Invalid model '" + paramValue + "'. Use 'default', 'simple', or 'reactive'");
            }
            model = paramValue;
        } else if (paramName == "history") {
            // History parameter accepts duration strings like "1w", "3d", "12h"
            // Validation will be done when actually using the value
            history = paramValue;
        } else {
            throw ExpressionParseException(
                "Unknown parameter '" + paramName + "'. Expected 'seasonality', 'model', or 'history'");
        }
    }

    consume(TokenType::RPAREN, "Expected ')' after forecast arguments");

    return ExpressionNode::makeForecastFunction(queryRef, algorithm, deviations, seasonality, model, history);
}

std::unique_ptr<ExpressionNode> ExpressionParser::parseFunctionCall(const std::string& name) {
    // Handle anomaly function specially (requires string literals)
    if (isAnomalyFunction(name)) {
        return parseAnomalyFunction();
    }

    // Handle forecast function specially (requires string literals)
    if (isForecastFunction(name)) {
        return parseForecastFunction();
    }

    consume(TokenType::LPAREN, "Expected '(' after function name");

    std::vector<std::unique_ptr<ExpressionNode>> args;

    // Parse arguments
    if (!check(TokenType::RPAREN)) {
        do {
            args.push_back(parseExpression());
        } while (match(TokenType::COMMA));
    }

    consume(TokenType::RPAREN, "Expected ')' after function arguments");

    // Check for unary functions
    auto unaryOp = getUnaryFunction(name);
    if (unaryOp.has_value()) {
        if (args.size() != 1) {
            throw ExpressionParseException(
                "Function '" + name + "' expects 1 argument, got " +
                std::to_string(args.size()));
        }
        return ExpressionNode::makeUnaryOp(unaryOp.value(), std::move(args[0]));
    }

    // Check for multi-arg functions
    auto multiFunc = getMultiArgFunction(name);
    if (multiFunc.has_value()) {
        // Validate argument count
        size_t expected = 2;
        if (multiFunc.value() == FunctionType::CLAMP) {
            expected = 3;
        }
        if (args.size() != expected) {
            throw ExpressionParseException(
                "Function '" + name + "' expects " + std::to_string(expected) +
                " arguments, got " + std::to_string(args.size()));
        }
        return ExpressionNode::makeFunctionCall(multiFunc.value(), std::move(args));
    }

    throw ExpressionParseException("Unknown function: " + name);
}

std::unique_ptr<ExpressionNode> ExpressionParser::parseGrouping() {
    consume(TokenType::LPAREN, "Expected '('");
    auto expr = parseExpression();
    consume(TokenType::RPAREN, "Expected ')'");
    return expr;
}

void ExpressionParser::validateQueryReferences(const std::set<std::string>& definedQueries) const {
    for (const auto& ref : queryRefs_) {
        if (definedQueries.find(ref) == definedQueries.end()) {
            throw ExpressionParseException(
                "Undefined query reference: '" + ref + "'");
        }
    }
}

} // namespace tsdb
