#include "../../../lib/query/query_parser.hpp"

#include <gtest/gtest.h>

#include <string>

using namespace timestar;

class QueryParserEdgeCaseTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// A 100KB measurement name should either parse or throw cleanly (no crash/hang).
TEST_F(QueryParserEdgeCaseTest, ExtremelyLongMeasurementName) {
    std::string longName(100 * 1024, 'x');
    std::string query = "avg:" + longName + "()";
    // Should not crash or hang regardless of outcome.
    try {
        auto result = QueryParser::parseQueryString(query);
        EXPECT_EQ(result.measurement, longName);
        EXPECT_EQ(result.aggregation, AggregationMethod::AVG);
        EXPECT_TRUE(result.requestsAllFields());
    } catch (const QueryParseException&) {
        // Throwing is also acceptable; the key assertion is no crash.
        SUCCEED();
    }
}

// 1000 comma-separated fields should parse successfully.
TEST_F(QueryParserEdgeCaseTest, ExtremelyLongFieldList) {
    std::string fields;
    for (int i = 0; i < 1000; ++i) {
        if (i > 0) fields += ',';
        fields += "field_" + std::to_string(i);
    }
    std::string query = "avg:m(" + fields + ")";
    auto result = QueryParser::parseQueryString(query);
    EXPECT_EQ(result.fields.size(), 1000u);
    EXPECT_EQ(result.fields[0], "field_0");
    EXPECT_EQ(result.fields[999], "field_999");
}

// 500 key:value scope pairs should parse successfully.
TEST_F(QueryParserEdgeCaseTest, ExtremelyLongScopeList) {
    std::string scopes;
    for (int i = 0; i < 500; ++i) {
        if (i > 0) scopes += ',';
        scopes += "key_" + std::to_string(i) + ":val_" + std::to_string(i);
    }
    std::string query = "avg:m(){" + scopes + "}";
    auto result = QueryParser::parseQueryString(query);
    EXPECT_EQ(result.scopes.size(), 500u);
    EXPECT_EQ(result.scopes.at("key_0"), "val_0");
    EXPECT_EQ(result.scopes.at("key_499"), "val_499");
}

// A query that is only whitespace should throw.
TEST_F(QueryParserEdgeCaseTest, OnlyWhitespace) {
    // After skipping whitespace, find(':') returns npos, so this throws.
    EXPECT_THROW(QueryParser::parseQueryString("   "), QueryParseException);
}

// An empty string should throw.
TEST_F(QueryParserEdgeCaseTest, EmptyString) {
    EXPECT_THROW(QueryParser::parseQueryString(""), QueryParseException);
}

// Double colon "avg::temperature()" -- the first colon splits correctly, so
// parseMeasurement reads ":temperature" as the measurement name.
TEST_F(QueryParserEdgeCaseTest, DoubleColonInQuery) {
    // "avg" is parsed as the aggregation (first colon is the separator).
    // After the first colon, measurement is read until '(' -- that yields ":temperature".
    // This is a valid (if odd) measurement name, so parsing succeeds.
    auto result = QueryParser::parseQueryString("avg::temperature()");
    EXPECT_EQ(result.aggregation, AggregationMethod::AVG);
    EXPECT_EQ(result.measurement, ":temperature");
}

// "by" used as a measurement name -- the parser does NOT confuse it with the
// group-by keyword because it is immediately followed by '(' (not space or '{').
TEST_F(QueryParserEdgeCaseTest, MeasurementNameIsByKeyword) {
    auto result = QueryParser::parseQueryString("avg:by()");
    EXPECT_EQ(result.measurement, "by");
    EXPECT_TRUE(result.requestsAllFields());
}

// No space between "by" and the opening brace should still parse.
TEST_F(QueryParserEdgeCaseTest, GroupByWithoutSpaceBeforeBrace) {
    auto result = QueryParser::parseQueryString("avg:m() by{tag}");
    EXPECT_EQ(result.measurement, "m");
    ASSERT_EQ(result.groupByTags.size(), 1u);
    EXPECT_EQ(result.groupByTags[0], "tag");
}

// Trailing characters after the group-by clause: the parser does not re-check
// for remaining input after successfully parsing a group-by clause, so trailing
// text is silently ignored. Verify the parsed result is correct and the garbage
// is simply discarded.
TEST_F(QueryParserEdgeCaseTest, TrailingGarbageAfterGroupBy) {
    auto result = QueryParser::parseQueryString("avg:m() by {tag}garbage");
    EXPECT_EQ(result.measurement, "m");
    ASSERT_EQ(result.groupByTags.size(), 1u);
    EXPECT_EQ(result.groupByTags[0], "tag");
}

// A scope value containing a colon should capture everything after the first
// colon as the value (e.g. "server:8080").
TEST_F(QueryParserEdgeCaseTest, ScopeValueContainsColon) {
    auto result = QueryParser::parseQueryString("avg:m(){host:server:8080}");
    ASSERT_EQ(result.scopes.size(), 1u);
    EXPECT_EQ(result.scopes.at("host"), "server:8080");
}

// Empty measurement name (nothing between colon and parenthesis) should throw.
TEST_F(QueryParserEdgeCaseTest, EmptyMeasurementThrows) {
    EXPECT_THROW(QueryParser::parseQueryString("avg:(value)"), QueryParseException);
}

// A trailing comma in the field list: the split loop terminates when start
// reaches the end of the string, so the trailing comma is silently ignored
// and only the valid field is captured.
TEST_F(QueryParserEdgeCaseTest, FieldListTrailingComma) {
    auto result = QueryParser::parseQueryString("avg:m(value,)");
    ASSERT_EQ(result.fields.size(), 1u);
    EXPECT_EQ(result.fields[0], "value");
}

// Year before 1970 is rejected by the range check.
TEST_F(QueryParserEdgeCaseTest, TimeYearBelow1970Throws) {
    EXPECT_THROW(QueryParser::parseTime("01-01-1969 00:00:00"), QueryParseException);
}

// A far-future date should parse without overflow (uint64_t has plenty of room).
TEST_F(QueryParserEdgeCaseTest, TimeFarFuture) {
    uint64_t ts = 0;
    EXPECT_NO_THROW(ts = QueryParser::parseTime("01-01-9999 00:00:00"));
    EXPECT_GT(ts, 0u);
    // Sanity: year 9999 should be well above year 2024.
    uint64_t ts2024 = QueryParser::parseTime("01-01-2024 00:00:00");
    EXPECT_GT(ts, ts2024);
}

// Feb 29 in a leap year (2024) is valid and should parse.
TEST_F(QueryParserEdgeCaseTest, TimeFeb29LeapYear) {
    uint64_t ts = 0;
    EXPECT_NO_THROW(ts = QueryParser::parseTime("29-02-2024 12:00:00"));
    EXPECT_GT(ts, 0u);
    // Should be between Feb 28 and Mar 1.
    uint64_t feb28 = QueryParser::parseTime("28-02-2024 12:00:00");
    uint64_t mar01 = QueryParser::parseTime("01-03-2024 12:00:00");
    EXPECT_GT(ts, feb28);
    EXPECT_LT(ts, mar01);
}

// An empty scope key (":value") should throw.
TEST_F(QueryParserEdgeCaseTest, ScopeWithEmptyKeyThrows) {
    EXPECT_THROW(QueryParser::parseQueryString("avg:m(){:value}"), QueryParseException);
}

// An empty scope value ("key:") should throw.
TEST_F(QueryParserEdgeCaseTest, ScopeWithEmptyValueThrows) {
    EXPECT_THROW(QueryParser::parseQueryString("avg:m(){key:}"), QueryParseException);
}

// Multiple scope key:value pairs should all be captured correctly.
TEST_F(QueryParserEdgeCaseTest, MultipleScopesAllCaptured) {
    auto result = QueryParser::parseQueryString(
        "avg:m(){region:us-east,env:prod,dc:dc-01,tier:frontend}");

    ASSERT_EQ(result.scopes.size(), 4u);
    EXPECT_EQ(result.scopes.at("region"), "us-east");
    EXPECT_EQ(result.scopes.at("env"), "prod");
    EXPECT_EQ(result.scopes.at("dc"), "dc-01");
    EXPECT_EQ(result.scopes.at("tier"), "frontend");
}
