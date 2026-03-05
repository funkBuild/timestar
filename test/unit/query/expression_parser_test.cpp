#include <gtest/gtest.h>
#include "expression_ast.hpp"
#include "expression_parser.hpp"

using namespace tsdb;

class ExpressionParserTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// ==================== Lexer Tests ====================

TEST_F(ExpressionParserTest, LexerBasicTokens) {
    ExpressionLexer lexer("a + b");

    auto t1 = lexer.nextToken();
    EXPECT_EQ(t1.type, TokenType::IDENTIFIER);
    EXPECT_EQ(t1.value, "a");

    auto t2 = lexer.nextToken();
    EXPECT_EQ(t2.type, TokenType::PLUS);

    auto t3 = lexer.nextToken();
    EXPECT_EQ(t3.type, TokenType::IDENTIFIER);
    EXPECT_EQ(t3.value, "b");

    auto t4 = lexer.nextToken();
    EXPECT_EQ(t4.type, TokenType::END);
}

TEST_F(ExpressionParserTest, LexerNumbers) {
    ExpressionLexer lexer("123 45.67 1e10 2.5e-3");

    auto t1 = lexer.nextToken();
    EXPECT_EQ(t1.type, TokenType::NUMBER);
    EXPECT_EQ(t1.value, "123");

    auto t2 = lexer.nextToken();
    EXPECT_EQ(t2.type, TokenType::NUMBER);
    EXPECT_EQ(t2.value, "45.67");

    auto t3 = lexer.nextToken();
    EXPECT_EQ(t3.type, TokenType::NUMBER);
    EXPECT_EQ(t3.value, "1e10");

    auto t4 = lexer.nextToken();
    EXPECT_EQ(t4.type, TokenType::NUMBER);
    EXPECT_EQ(t4.value, "2.5e-3");
}

TEST_F(ExpressionParserTest, LexerAllOperators) {
    ExpressionLexer lexer("+ - * / ( ) ,");

    EXPECT_EQ(lexer.nextToken().type, TokenType::PLUS);
    EXPECT_EQ(lexer.nextToken().type, TokenType::MINUS);
    EXPECT_EQ(lexer.nextToken().type, TokenType::STAR);
    EXPECT_EQ(lexer.nextToken().type, TokenType::SLASH);
    EXPECT_EQ(lexer.nextToken().type, TokenType::LPAREN);
    EXPECT_EQ(lexer.nextToken().type, TokenType::RPAREN);
    EXPECT_EQ(lexer.nextToken().type, TokenType::COMMA);
    EXPECT_EQ(lexer.nextToken().type, TokenType::END);
}

TEST_F(ExpressionParserTest, LexerIdentifiersWithUnderscores) {
    ExpressionLexer lexer("query_a query_123 _private");

    auto t1 = lexer.nextToken();
    EXPECT_EQ(t1.value, "query_a");

    auto t2 = lexer.nextToken();
    EXPECT_EQ(t2.value, "query_123");

    auto t3 = lexer.nextToken();
    EXPECT_EQ(t3.value, "_private");
}

TEST_F(ExpressionParserTest, LexerInvalidCharacter) {
    ExpressionLexer lexer("a @ b");
    lexer.nextToken(); // a

    EXPECT_THROW(lexer.nextToken(), ExpressionParseException);
}

// ==================== Simple Expression Tests ====================

TEST_F(ExpressionParserTest, ParseQueryRef) {
    ExpressionParser parser("a");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::QUERY_REF);
    EXPECT_EQ(ast->asQueryRef().name, "a");
    EXPECT_EQ(ast->toString(), "a");
}

TEST_F(ExpressionParserTest, ParseScalar) {
    ExpressionParser parser("42.5");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::SCALAR);
    EXPECT_DOUBLE_EQ(ast->asScalar().value, 42.5);
}

TEST_F(ExpressionParserTest, ParseScalarScientific) {
    ExpressionParser parser("1.5e6");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::SCALAR);
    EXPECT_DOUBLE_EQ(ast->asScalar().value, 1500000.0);
}

// ==================== Binary Operator Tests ====================

TEST_F(ExpressionParserTest, ParseSimpleAddition) {
    ExpressionParser parser("a + b");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::BINARY_OP);
    const auto& op = ast->asBinaryOp();
    EXPECT_EQ(op.op, BinaryOpType::ADD);
    EXPECT_EQ(op.left->asQueryRef().name, "a");
    EXPECT_EQ(op.right->asQueryRef().name, "b");
}

TEST_F(ExpressionParserTest, ParseSimpleSubtraction) {
    ExpressionParser parser("a - b");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::BINARY_OP);
    EXPECT_EQ(ast->asBinaryOp().op, BinaryOpType::SUBTRACT);
}

TEST_F(ExpressionParserTest, ParseSimpleMultiplication) {
    ExpressionParser parser("a * b");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::BINARY_OP);
    EXPECT_EQ(ast->asBinaryOp().op, BinaryOpType::MULTIPLY);
}

TEST_F(ExpressionParserTest, ParseSimpleDivision) {
    ExpressionParser parser("a / b");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::BINARY_OP);
    EXPECT_EQ(ast->asBinaryOp().op, BinaryOpType::DIVIDE);
}

TEST_F(ExpressionParserTest, ParseScalarMultiplication) {
    ExpressionParser parser("a * 100");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::BINARY_OP);
    const auto& op = ast->asBinaryOp();
    EXPECT_EQ(op.op, BinaryOpType::MULTIPLY);
    EXPECT_EQ(op.left->type, ExprNodeType::QUERY_REF);
    EXPECT_EQ(op.right->type, ExprNodeType::SCALAR);
    EXPECT_DOUBLE_EQ(op.right->asScalar().value, 100.0);
}

// ==================== Operator Precedence Tests ====================

TEST_F(ExpressionParserTest, PrecedenceMultiplicationBeforeAddition) {
    // a + b * c should parse as a + (b * c)
    ExpressionParser parser("a + b * c");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::BINARY_OP);
    const auto& op = ast->asBinaryOp();
    EXPECT_EQ(op.op, BinaryOpType::ADD);
    EXPECT_EQ(op.left->asQueryRef().name, "a");

    // Right side should be b * c
    EXPECT_EQ(op.right->type, ExprNodeType::BINARY_OP);
    const auto& rightOp = op.right->asBinaryOp();
    EXPECT_EQ(rightOp.op, BinaryOpType::MULTIPLY);
    EXPECT_EQ(rightOp.left->asQueryRef().name, "b");
    EXPECT_EQ(rightOp.right->asQueryRef().name, "c");
}

TEST_F(ExpressionParserTest, PrecedenceDivisionBeforeSubtraction) {
    // a - b / c should parse as a - (b / c)
    ExpressionParser parser("a - b / c");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::BINARY_OP);
    const auto& op = ast->asBinaryOp();
    EXPECT_EQ(op.op, BinaryOpType::SUBTRACT);
    EXPECT_EQ(op.right->type, ExprNodeType::BINARY_OP);
    EXPECT_EQ(op.right->asBinaryOp().op, BinaryOpType::DIVIDE);
}

TEST_F(ExpressionParserTest, PrecedenceLeftAssociative) {
    // a - b - c should parse as (a - b) - c
    ExpressionParser parser("a - b - c");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::BINARY_OP);
    const auto& op = ast->asBinaryOp();
    EXPECT_EQ(op.op, BinaryOpType::SUBTRACT);
    EXPECT_EQ(op.right->asQueryRef().name, "c");

    // Left side should be a - b
    EXPECT_EQ(op.left->type, ExprNodeType::BINARY_OP);
    const auto& leftOp = op.left->asBinaryOp();
    EXPECT_EQ(leftOp.left->asQueryRef().name, "a");
    EXPECT_EQ(leftOp.right->asQueryRef().name, "b");
}

TEST_F(ExpressionParserTest, PrecedenceWithParentheses) {
    // (a + b) * c should respect parentheses
    ExpressionParser parser("(a + b) * c");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::BINARY_OP);
    const auto& op = ast->asBinaryOp();
    EXPECT_EQ(op.op, BinaryOpType::MULTIPLY);
    EXPECT_EQ(op.right->asQueryRef().name, "c");

    // Left side should be a + b
    EXPECT_EQ(op.left->type, ExprNodeType::BINARY_OP);
    EXPECT_EQ(op.left->asBinaryOp().op, BinaryOpType::ADD);
}

// ==================== Unary Operator Tests ====================

TEST_F(ExpressionParserTest, ParseUnaryMinus) {
    ExpressionParser parser("-a");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::UNARY_OP);
    const auto& op = ast->asUnaryOp();
    EXPECT_EQ(op.op, UnaryOpType::NEGATE);
    EXPECT_EQ(op.operand->asQueryRef().name, "a");
}

TEST_F(ExpressionParserTest, ParseUnaryMinusWithExpression) {
    ExpressionParser parser("a + -b");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::BINARY_OP);
    const auto& op = ast->asBinaryOp();
    EXPECT_EQ(op.right->type, ExprNodeType::UNARY_OP);
    EXPECT_EQ(op.right->asUnaryOp().op, UnaryOpType::NEGATE);
}

TEST_F(ExpressionParserTest, ParseUnaryPlus) {
    // Unary plus should be ignored
    ExpressionParser parser("+a");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::QUERY_REF);
    EXPECT_EQ(ast->asQueryRef().name, "a");
}

// ==================== Function Call Tests ====================

TEST_F(ExpressionParserTest, ParseAbsFunction) {
    ExpressionParser parser("abs(a)");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::UNARY_OP);
    const auto& op = ast->asUnaryOp();
    EXPECT_EQ(op.op, UnaryOpType::ABS);
    EXPECT_EQ(op.operand->asQueryRef().name, "a");
}

TEST_F(ExpressionParserTest, ParseLogFunction) {
    ExpressionParser parser("log(a)");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::UNARY_OP);
    EXPECT_EQ(ast->asUnaryOp().op, UnaryOpType::LOG);
}

TEST_F(ExpressionParserTest, ParseSqrtFunction) {
    ExpressionParser parser("sqrt(a * b)");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::UNARY_OP);
    EXPECT_EQ(ast->asUnaryOp().op, UnaryOpType::SQRT);
    EXPECT_EQ(ast->asUnaryOp().operand->type, ExprNodeType::BINARY_OP);
}

TEST_F(ExpressionParserTest, ParseMinFunction) {
    ExpressionParser parser("min(a, b)");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::FUNCTION_CALL);
    const auto& call = ast->asFunctionCall();
    EXPECT_EQ(call.func, FunctionType::MIN);
    EXPECT_EQ(call.args.size(), 2);
    EXPECT_EQ(call.args[0]->asQueryRef().name, "a");
    EXPECT_EQ(call.args[1]->asQueryRef().name, "b");
}

TEST_F(ExpressionParserTest, ParseMaxFunction) {
    ExpressionParser parser("max(a, b)");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::FUNCTION_CALL);
    EXPECT_EQ(ast->asFunctionCall().func, FunctionType::MAX);
}

TEST_F(ExpressionParserTest, ParsePowFunction) {
    ExpressionParser parser("pow(a, 2)");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::FUNCTION_CALL);
    const auto& call = ast->asFunctionCall();
    EXPECT_EQ(call.func, FunctionType::POW);
    EXPECT_EQ(call.args[1]->asScalar().value, 2.0);
}

TEST_F(ExpressionParserTest, ParseClampFunction) {
    ExpressionParser parser("clamp(a, 0, 100)");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::FUNCTION_CALL);
    const auto& call = ast->asFunctionCall();
    EXPECT_EQ(call.func, FunctionType::CLAMP);
    EXPECT_EQ(call.args.size(), 3);
}

TEST_F(ExpressionParserTest, ParseNestedFunctions) {
    ExpressionParser parser("abs(min(a, b))");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::UNARY_OP);
    EXPECT_EQ(ast->asUnaryOp().op, UnaryOpType::ABS);
    EXPECT_EQ(ast->asUnaryOp().operand->type, ExprNodeType::FUNCTION_CALL);
}

// ==================== Complex Expression Tests ====================

TEST_F(ExpressionParserTest, ParseDerivedMetricFormula) {
    // Real-world example: (a / (a + b)) * 100
    ExpressionParser parser("(a / (a + b)) * 100");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::BINARY_OP);
    const auto& outer = ast->asBinaryOp();
    EXPECT_EQ(outer.op, BinaryOpType::MULTIPLY);
    EXPECT_EQ(outer.right->asScalar().value, 100.0);

    // Left side: a / (a + b)
    const auto& division = outer.left->asBinaryOp();
    EXPECT_EQ(division.op, BinaryOpType::DIVIDE);
    EXPECT_EQ(division.left->asQueryRef().name, "a");

    // Inner: a + b
    const auto& addition = division.right->asBinaryOp();
    EXPECT_EQ(addition.op, BinaryOpType::ADD);
}

TEST_F(ExpressionParserTest, ParseErrorRateFormula) {
    // errors / total * 100
    ExpressionParser parser("errors / total * 100");
    auto ast = parser.parse();

    // Should be (errors / total) * 100 due to left-to-right associativity
    EXPECT_EQ(ast->type, ExprNodeType::BINARY_OP);
    EXPECT_EQ(ast->asBinaryOp().op, BinaryOpType::MULTIPLY);
}

TEST_F(ExpressionParserTest, ParseComplexFormula) {
    // max(a, b) / (min(c, d) + 1)
    ExpressionParser parser("max(a, b) / (min(c, d) + 1)");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::BINARY_OP);
    const auto& div = ast->asBinaryOp();
    EXPECT_EQ(div.op, BinaryOpType::DIVIDE);
    EXPECT_EQ(div.left->type, ExprNodeType::FUNCTION_CALL);
    EXPECT_EQ(div.right->type, ExprNodeType::BINARY_OP);
}

// ==================== Query Reference Tracking Tests ====================

TEST_F(ExpressionParserTest, TrackQueryReferences) {
    ExpressionParser parser("a + b * c - d");
    parser.parse();

    auto refs = parser.getQueryReferences();
    EXPECT_EQ(refs.size(), 4);
    EXPECT_TRUE(refs.count("a"));
    EXPECT_TRUE(refs.count("b"));
    EXPECT_TRUE(refs.count("c"));
    EXPECT_TRUE(refs.count("d"));
}

TEST_F(ExpressionParserTest, TrackQueryReferencesWithFunctions) {
    ExpressionParser parser("max(a, b) + min(c, d)");
    parser.parse();

    auto refs = parser.getQueryReferences();
    EXPECT_EQ(refs.size(), 4);
}

TEST_F(ExpressionParserTest, ValidateQueryReferencesSuccess) {
    ExpressionParser parser("a + b");
    parser.parse();

    std::set<std::string> defined = {"a", "b", "c"};
    EXPECT_NO_THROW(parser.validateQueryReferences(defined));
}

TEST_F(ExpressionParserTest, ValidateQueryReferencesFailure) {
    ExpressionParser parser("a + b + undefined");
    parser.parse();

    std::set<std::string> defined = {"a", "b"};
    EXPECT_THROW(parser.validateQueryReferences(defined), ExpressionParseException);
}

// ==================== Error Handling Tests ====================

TEST_F(ExpressionParserTest, ErrorEmptyExpression) {
    ExpressionParser parser("");
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}

TEST_F(ExpressionParserTest, ErrorMissingOperand) {
    ExpressionParser parser("a +");
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}

TEST_F(ExpressionParserTest, ErrorUnmatchedParenthesis) {
    ExpressionParser parser("(a + b");
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}

TEST_F(ExpressionParserTest, ErrorExtraParenthesis) {
    ExpressionParser parser("a + b)");
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}

TEST_F(ExpressionParserTest, ErrorUnknownFunction) {
    ExpressionParser parser("unknown(a)");
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}

TEST_F(ExpressionParserTest, ErrorWrongArgumentCount) {
    ExpressionParser parser("abs(a, b)");
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}

TEST_F(ExpressionParserTest, ErrorMinWrongArgCount) {
    ExpressionParser parser("min(a)");
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}

TEST_F(ExpressionParserTest, ErrorClampWrongArgCount) {
    ExpressionParser parser("clamp(a, b)");
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}

// ==================== ToString Tests ====================

TEST_F(ExpressionParserTest, ToStringSimple) {
    ExpressionParser parser("a + b");
    auto ast = parser.parse();
    EXPECT_EQ(ast->toString(), "(a + b)");
}

TEST_F(ExpressionParserTest, ToStringComplex) {
    ExpressionParser parser("(a + b) * c");
    auto ast = parser.parse();
    EXPECT_EQ(ast->toString(), "((a + b) * c)");
}

TEST_F(ExpressionParserTest, ToStringWithFunction) {
    ExpressionParser parser("min(a, b)");
    auto ast = parser.parse();
    EXPECT_EQ(ast->toString(), "min(a, b)");
}

TEST_F(ExpressionParserTest, ToStringNegation) {
    ExpressionParser parser("-a");
    auto ast = parser.parse();
    EXPECT_EQ(ast->toString(), "(-a)");
}

// ==================== Lexer String Literal Tests ====================

TEST_F(ExpressionParserTest, LexerStringLiteral) {
    ExpressionLexer lexer("'basic'");

    auto t1 = lexer.nextToken();
    EXPECT_EQ(t1.type, TokenType::STRING);
    EXPECT_EQ(t1.value, "basic");
}

TEST_F(ExpressionParserTest, LexerMultipleStringLiterals) {
    ExpressionLexer lexer("'basic' 'daily'");

    auto t1 = lexer.nextToken();
    EXPECT_EQ(t1.type, TokenType::STRING);
    EXPECT_EQ(t1.value, "basic");

    auto t2 = lexer.nextToken();
    EXPECT_EQ(t2.type, TokenType::STRING);
    EXPECT_EQ(t2.value, "daily");
}

TEST_F(ExpressionParserTest, LexerUnterminatedString) {
    ExpressionLexer lexer("'unterminated");
    EXPECT_THROW(lexer.nextToken(), ExpressionParseException);
}

// ==================== Anomaly Function Tests ====================

TEST_F(ExpressionParserTest, ParseAnomalyFunctionBasic) {
    ExpressionParser parser("anomalies(cpu, 'basic', 2)");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::ANOMALY_FUNCTION);
    const auto& anomaly = ast->asAnomalyFunction();
    EXPECT_EQ(anomaly.queryRef, "cpu");
    EXPECT_EQ(anomaly.algorithm, "basic");
    EXPECT_DOUBLE_EQ(anomaly.bounds, 2.0);
    EXPECT_FALSE(anomaly.seasonality.has_value());
}

TEST_F(ExpressionParserTest, ParseAnomalyFunctionAgile) {
    ExpressionParser parser("anomalies(memory, 'agile', 3)");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::ANOMALY_FUNCTION);
    const auto& anomaly = ast->asAnomalyFunction();
    EXPECT_EQ(anomaly.queryRef, "memory");
    EXPECT_EQ(anomaly.algorithm, "agile");
    EXPECT_DOUBLE_EQ(anomaly.bounds, 3.0);
}

TEST_F(ExpressionParserTest, ParseAnomalyFunctionRobust) {
    ExpressionParser parser("anomalies(disk, 'robust', 2.5)");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::ANOMALY_FUNCTION);
    const auto& anomaly = ast->asAnomalyFunction();
    EXPECT_EQ(anomaly.queryRef, "disk");
    EXPECT_EQ(anomaly.algorithm, "robust");
    EXPECT_DOUBLE_EQ(anomaly.bounds, 2.5);
}

TEST_F(ExpressionParserTest, ParseAnomalyFunctionWithHourlySeasonality) {
    ExpressionParser parser("anomalies(cpu, 'agile', 2, 'hourly')");
    auto ast = parser.parse();

    EXPECT_EQ(ast->type, ExprNodeType::ANOMALY_FUNCTION);
    const auto& anomaly = ast->asAnomalyFunction();
    EXPECT_EQ(anomaly.queryRef, "cpu");
    EXPECT_EQ(anomaly.algorithm, "agile");
    EXPECT_DOUBLE_EQ(anomaly.bounds, 2.0);
    ASSERT_TRUE(anomaly.seasonality.has_value());
    EXPECT_EQ(anomaly.seasonality.value(), "hourly");
}

TEST_F(ExpressionParserTest, ParseAnomalyFunctionWithDailySeasonality) {
    ExpressionParser parser("anomalies(network, 'robust', 2, 'daily')");
    auto ast = parser.parse();

    const auto& anomaly = ast->asAnomalyFunction();
    EXPECT_EQ(anomaly.seasonality.value(), "daily");
}

TEST_F(ExpressionParserTest, ParseAnomalyFunctionWithWeeklySeasonality) {
    ExpressionParser parser("anomalies(traffic, 'robust', 3, 'weekly')");
    auto ast = parser.parse();

    const auto& anomaly = ast->asAnomalyFunction();
    EXPECT_EQ(anomaly.seasonality.value(), "weekly");
}

TEST_F(ExpressionParserTest, ParseAnomalyTracksQueryRef) {
    ExpressionParser parser("anomalies(myquery, 'basic', 2)");
    parser.parse();

    auto refs = parser.getQueryReferences();
    EXPECT_EQ(refs.size(), 1);
    EXPECT_TRUE(refs.count("myquery"));
}

TEST_F(ExpressionParserTest, ErrorAnomalyInvalidAlgorithm) {
    ExpressionParser parser("anomalies(cpu, 'invalid', 2)");
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}

TEST_F(ExpressionParserTest, ErrorAnomalyBoundsTooLow) {
    ExpressionParser parser("anomalies(cpu, 'basic', 0.5)");
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}

TEST_F(ExpressionParserTest, ErrorAnomalyBoundsTooHigh) {
    ExpressionParser parser("anomalies(cpu, 'basic', 5)");
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}

TEST_F(ExpressionParserTest, ErrorAnomalyInvalidSeasonality) {
    ExpressionParser parser("anomalies(cpu, 'agile', 2, 'monthly')");
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}

TEST_F(ExpressionParserTest, ErrorAnomalyMissingArguments) {
    ExpressionParser parser("anomalies(cpu, 'basic')");
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}

TEST_F(ExpressionParserTest, ErrorAnomalyMissingQueryRef) {
    ExpressionParser parser("anomalies('basic', 2)");
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}

TEST_F(ExpressionParserTest, ToStringAnomalyFunction) {
    ExpressionParser parser("anomalies(cpu, 'basic', 2)");
    auto ast = parser.parse();
    std::string str = ast->toString();
    EXPECT_TRUE(str.find("anomalies(") != std::string::npos);
    EXPECT_TRUE(str.find("cpu") != std::string::npos);
    EXPECT_TRUE(str.find("basic") != std::string::npos);
}

TEST_F(ExpressionParserTest, ToStringAnomalyFunctionWithSeasonality) {
    ExpressionParser parser("anomalies(cpu, 'agile', 2, 'daily')");
    auto ast = parser.parse();
    std::string str = ast->toString();
    EXPECT_TRUE(str.find("daily") != std::string::npos);
}

// ==================== time_shift() Parser Tests ====================

TEST_F(ExpressionParserTest, TimeShiftBasicForward) {
    // time_shift(a, '7d') parses to a TIME_SHIFT_FUNCTION node
    ExpressionParser parser("time_shift(a, '7d')");
    auto ast = parser.parse();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->type, ExprNodeType::TIME_SHIFT_FUNCTION);
    const auto& ts = ast->asTimeShiftFunction();
    EXPECT_EQ(ts.queryRef, "a");
    EXPECT_EQ(ts.offset, "7d");
}

TEST_F(ExpressionParserTest, TimeShiftNegativeOffset) {
    // Negative offset is stored as-is with the leading '-' in the string
    ExpressionParser parser("time_shift(a, '-1h')");
    auto ast = parser.parse();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->type, ExprNodeType::TIME_SHIFT_FUNCTION);
    const auto& ts = ast->asTimeShiftFunction();
    EXPECT_EQ(ts.queryRef, "a");
    EXPECT_EQ(ts.offset, "-1h");
}

TEST_F(ExpressionParserTest, TimeShiftQueryRefRegistered) {
    // The query reference used in time_shift should be registered
    ExpressionParser parser("time_shift(my_query, '30m')");
    auto ast = parser.parse();
    auto refs = parser.getQueryReferences();
    EXPECT_TRUE(refs.count("my_query") > 0);
}

TEST_F(ExpressionParserTest, TimeShiftToString) {
    ExpressionParser parser("time_shift(a, '7d')");
    auto ast = parser.parse();
    std::string str = ast->toString();
    EXPECT_TRUE(str.find("time_shift(") != std::string::npos);
    EXPECT_TRUE(str.find("a") != std::string::npos);
    EXPECT_TRUE(str.find("7d") != std::string::npos);
}

TEST_F(ExpressionParserTest, TimeShiftInBinaryExpression) {
    // a - time_shift(a, '7d') should parse as a subtraction
    ExpressionParser parser("a - time_shift(a, '7d')");
    auto ast = parser.parse();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->type, ExprNodeType::BINARY_OP);
    const auto& op = ast->asBinaryOp();
    EXPECT_EQ(op.op, BinaryOpType::SUBTRACT);
    EXPECT_EQ(op.right->type, ExprNodeType::TIME_SHIFT_FUNCTION);
}

TEST_F(ExpressionParserTest, ErrorTimeShiftMissingSecondArg) {
    ExpressionParser parser("time_shift(a)");
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}

TEST_F(ExpressionParserTest, ErrorTimeShiftSecondArgNotString) {
    ExpressionParser parser("time_shift(a, 7)");
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}

TEST_F(ExpressionParserTest, ErrorTimeShiftMissingQueryRef) {
    ExpressionParser parser("time_shift('7d')");
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}

// ==================== Depth Limit Tests ====================

TEST_F(ExpressionParserTest, ExpressionParser_DeepNesting_ThrowsError) {
    // Build a deeply nested expression: ((((... (a) ...))))
    // 600 levels of parentheses wrapping a single query reference.
    // Without a depth guard this would overflow the call stack and segfault.
    const int depth = 600;
    std::string expr(depth, '(');
    expr += "a";
    expr += std::string(depth, ')');

    ExpressionParser parser(expr);
    EXPECT_THROW(parser.parse(), std::exception);
}

// ==================== Argument Count Limit Tests ====================

TEST_F(ExpressionParserTest, FunctionArgLimit_ExactlyAtLimit_Accepted) {
    // avg_of_series is variadic and accepts >= 1 argument.
    // Build a call with exactly MAX_ARGS (1000) arguments — must be accepted.
    // Each argument is the scalar "1", separated by commas.
    std::string expr = "avg_of_series(";
    for (int i = 0; i < 1000; ++i) {
        if (i > 0) expr += ",";
        expr += "1";
    }
    expr += ")";

    ExpressionParser parser(expr);
    EXPECT_NO_THROW(parser.parse());
}

TEST_F(ExpressionParserTest, FunctionArgLimit_OneOverLimit_Throws) {
    // Build a call with 1001 arguments — one over the MAX_ARGS limit.
    // The parser must reject this to prevent OOM from unbounded argument lists.
    std::string expr = "avg_of_series(";
    for (int i = 0; i < 1001; ++i) {
        if (i > 0) expr += ",";
        expr += "1";
    }
    expr += ")";

    ExpressionParser parser(expr);
    EXPECT_THROW(parser.parse(), ExpressionParseException);
}
