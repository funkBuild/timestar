/*
 * Tests for FunctionQueryParser - verifying whole-word function name matching.
 *
 * The parser must detect function names as whole words only.  The original
 * implementation used std::string::find() which matches any substring, so a
 * query containing "checksum" was incorrectly identified as containing "sma",
 * and a query containing "schema" was incorrectly identified as containing "ema".
 */

#include "functions/function_query_parser.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

using namespace timestar::functions;

class FunctionQueryParserTest : public ::testing::Test {
protected:
    FunctionQueryParser parser;

    bool hasFunction(const std::vector<std::string>& result, const std::string& name) {
        return std::find(result.begin(), result.end(), name) != result.end();
    }
};

// ---------------------------------------------------------------------------
// Positive cases: function names that ARE present as whole words
// ---------------------------------------------------------------------------

TEST_F(FunctionQueryParserTest, DetectsSmaAsWholeWord) {
    auto result = parser.parse("sma(field, 5)");
    EXPECT_TRUE(hasFunction(result, "sma")) << "Should detect 'sma' when present as a whole word";
}

TEST_F(FunctionQueryParserTest, DetectsEmaAsWholeWord) {
    auto result = parser.parse("ema(field, 5)");
    EXPECT_TRUE(hasFunction(result, "ema")) << "Should detect 'ema' when present as a whole word";
}

TEST_F(FunctionQueryParserTest, DetectsAddAsWholeWord) {
    auto result = parser.parse("add(field1, field2)");
    EXPECT_TRUE(hasFunction(result, "add")) << "Should detect 'add' when present as a whole word";
}

TEST_F(FunctionQueryParserTest, DetectsMultiplyAsWholeWord) {
    auto result = parser.parse("multiply(field, 2.0)");
    EXPECT_TRUE(hasFunction(result, "multiply")) << "Should detect 'multiply' when present as whole word";
}

// ---------------------------------------------------------------------------
// Negative cases: substrings that must NOT trigger a false positive
// ---------------------------------------------------------------------------

// "checksum" contains "sma" but is NOT the sma function
TEST_F(FunctionQueryParserTest, NoFalsePositiveSmaInChecksum) {
    auto result = parser.parse("checksum(field)");
    EXPECT_FALSE(hasFunction(result, "sma")) << "'checksum' must NOT be recognised as containing the 'sma' function";
}

// "schema" contains "ema" but is NOT the ema function
TEST_F(FunctionQueryParserTest, NoFalsePositiveEmaInSchema) {
    auto result = parser.parse("schema_field");
    EXPECT_FALSE(hasFunction(result, "ema"))
        << "'schema_field' must NOT be recognised as containing the 'ema' function";
}

// "cinema" contains "ema" — another substring collision case
TEST_F(FunctionQueryParserTest, NoFalsePositiveEmaInCinema) {
    auto result = parser.parse("cinema");
    EXPECT_FALSE(hasFunction(result, "ema")) << "'cinema' must NOT be recognised as containing the 'ema' function";
}

// "address" contains "add" but is NOT the add function
TEST_F(FunctionQueryParserTest, NoFalsePositiveAddInAddress) {
    auto result = parser.parse("address_field");
    EXPECT_FALSE(hasFunction(result, "add"))
        << "'address_field' must NOT be recognised as containing the 'add' function";
}

// "padded" contains "add" but is NOT the add function
TEST_F(FunctionQueryParserTest, NoFalsePositiveAddInPadded) {
    auto result = parser.parse("padded");
    EXPECT_FALSE(hasFunction(result, "add")) << "'padded' must NOT be recognised as containing the 'add' function";
}

// "summary" contains "sma" — substring match that should not fire
TEST_F(FunctionQueryParserTest, NoFalsePositiveSmaInSummary) {
    auto result = parser.parse("summary_stats");
    EXPECT_FALSE(hasFunction(result, "sma"))
        << "'summary_stats' must NOT be recognised as containing the 'sma' function";
}

// ---------------------------------------------------------------------------
// Edge cases: function names at start/end of string and next to punctuation
// ---------------------------------------------------------------------------

TEST_F(FunctionQueryParserTest, DetectsSmaAtStartOfString) {
    auto result = parser.parse("sma");
    EXPECT_TRUE(hasFunction(result, "sma")) << "Should detect 'sma' at start of string";
}

TEST_F(FunctionQueryParserTest, DetectsEmaAtEndOfString) {
    auto result = parser.parse("field ema");
    EXPECT_TRUE(hasFunction(result, "ema")) << "Should detect 'ema' at end of string";
}

TEST_F(FunctionQueryParserTest, DetectsSmaNextToPunctuation) {
    auto result = parser.parse("(sma,5)");
    EXPECT_TRUE(hasFunction(result, "sma")) << "Should detect 'sma' next to punctuation";
}

TEST_F(FunctionQueryParserTest, EmptyQueryReturnsNoFunctions) {
    auto result = parser.parse("");
    EXPECT_TRUE(result.empty()) << "Empty query should return no functions";
}
