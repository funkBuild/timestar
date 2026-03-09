#ifndef EXPRESSION_PARSER_H_INCLUDED
#define EXPRESSION_PARSER_H_INCLUDED

#include "expression_ast.hpp"
#include <string>
#include <string_view>
#include <optional>
#include <set>

namespace timestar {

// Token types for lexer
enum class TokenType {
    END,            // End of input
    NUMBER,         // 123, 45.67
    IDENTIFIER,     // a, b, query_name, abs, min, max
    STRING,         // 'basic', 'robust', etc.
    PLUS,           // +
    MINUS,          // -
    STAR,           // *
    SLASH,          // /
    LPAREN,         // (
    RPAREN,         // )
    COMMA,          // ,
    EQUALS          // =
};

struct Token {
    TokenType type;
    std::string value;
    size_t position;

    Token(TokenType t, std::string v, size_t pos)
        : type(t), value(std::move(v)), position(pos) {}
};

// Lexer for tokenizing expression strings
class ExpressionLexer {
public:
    explicit ExpressionLexer(std::string_view input);

    Token nextToken();
    Token peekToken();
    size_t currentPosition() const { return pos_; }

private:
    std::string_view input_;
    size_t pos_ = 0;
    std::optional<Token> peeked_;

    void skipWhitespace();
    Token readNumber();
    Token readIdentifier();
    Token readString();
    char peek() const;
    char advance();
    bool isAtEnd() const;
};

// Pratt parser for parsing expressions with correct operator precedence
class ExpressionParser {
public:
    explicit ExpressionParser(std::string_view input);

    // Parse the entire expression and return the AST root
    std::unique_ptr<ExpressionNode> parse();

    // Get all query references used in the expression
    std::set<std::string> getQueryReferences() const { return queryRefs_; }

    // Validate that all query references in the expression are defined
    void validateQueryReferences(const std::set<std::string>& definedQueries) const;

private:
    ExpressionLexer lexer_;
    Token currentToken_;
    std::set<std::string> queryRefs_;
    int depth_ = 0;
    static constexpr int MAX_DEPTH = 100;
    static constexpr size_t MAX_ARGS = 1000;

    // Operator precedence levels
    static constexpr int PREC_NONE = 0;
    static constexpr int PREC_TERM = 1;      // + -
    static constexpr int PREC_FACTOR = 2;    // * /
    static constexpr int PREC_UNARY = 3;     // - (negation)
    static constexpr int PREC_CALL = 4;      // function calls

    void advance();
    bool check(TokenType type) const;
    bool match(TokenType type);
    void consume(TokenType type, const std::string& message);

    // Pratt parsing methods
    std::unique_ptr<ExpressionNode> parseExpression(int precedence = PREC_NONE);
    std::unique_ptr<ExpressionNode> parsePrimary();
    std::unique_ptr<ExpressionNode> parseUnary();
    std::unique_ptr<ExpressionNode> parseGrouping();
    std::unique_ptr<ExpressionNode> parseNumber();
    std::unique_ptr<ExpressionNode> parseIdentifier();
    std::unique_ptr<ExpressionNode> parseFunctionCall(const std::string& name);

    // Get precedence of current token
    int getPrecedence() const;

    // Get binary operator type from token
    std::optional<BinaryOpType> getBinaryOp() const;

    // Check if identifier is a known function
    bool isFunction(const std::string& name) const;
    bool isAnomalyFunction(const std::string& name) const;
    bool isForecastFunction(const std::string& name) const;
    bool isTimeShiftFunction(const std::string& name) const;
    std::optional<UnaryOpType> getUnaryFunction(const std::string& name) const;
    std::optional<FunctionType> getMultiArgFunction(const std::string& name) const;

    // Special function parsing
    std::unique_ptr<ExpressionNode> parseAnomalyFunction();
    std::unique_ptr<ExpressionNode> parseForecastFunction();
    std::unique_ptr<ExpressionNode> parseTimeShiftFunction();

    [[noreturn]] void error(const std::string& message) const;
};

} // namespace timestar

#endif // EXPRESSION_PARSER_H_INCLUDED
