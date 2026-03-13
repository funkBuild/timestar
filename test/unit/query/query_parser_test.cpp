#include "../../../lib/query/query_parser.hpp"

#include <gtest/gtest.h>

using namespace timestar;

class QueryParserTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// Test basic query parsing
TEST_F(QueryParserTest, ParseSimpleQuery) {
    std::string query = "avg:temperature()";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "02-01-2024 00:00:00";

    QueryRequest request = QueryParser::parse(query, startTime, endTime);

    EXPECT_EQ(request.aggregation, AggregationMethod::AVG);
    EXPECT_EQ(request.measurement, "temperature");
    EXPECT_TRUE(request.requestsAllFields());
    EXPECT_TRUE(request.hasNoFilters());
    EXPECT_FALSE(request.hasGroupBy());
}

// Test query with specific fields
TEST_F(QueryParserTest, ParseQueryWithFields) {
    std::string query = "max:cpu(usage_percent,idle_percent)";

    QueryRequest request = QueryParser::parseQueryString(query);

    EXPECT_EQ(request.aggregation, AggregationMethod::MAX);
    EXPECT_EQ(request.measurement, "cpu");
    EXPECT_EQ(request.fields.size(), 2);
    EXPECT_EQ(request.fields[0], "usage_percent");
    EXPECT_EQ(request.fields[1], "idle_percent");
}

// Test query with scopes (filters)
TEST_F(QueryParserTest, ParseQueryWithScopes) {
    std::string query = "min:temperature(value){location:us-west,sensor:temp-01}";

    QueryRequest request = QueryParser::parseQueryString(query);

    EXPECT_EQ(request.aggregation, AggregationMethod::MIN);
    EXPECT_EQ(request.measurement, "temperature");
    EXPECT_EQ(request.fields.size(), 1);
    EXPECT_EQ(request.fields[0], "value");
    EXPECT_EQ(request.scopes.size(), 2);
    EXPECT_EQ(request.scopes.at("location"), "us-west");
    EXPECT_EQ(request.scopes.at("sensor"), "temp-01");
}

// Test query with group by
TEST_F(QueryParserTest, ParseQueryWithGroupBy) {
    std::string query = "sum:sales(amount){region:north} by {product,category}";

    QueryRequest request = QueryParser::parseQueryString(query);

    EXPECT_EQ(request.aggregation, AggregationMethod::SUM);
    EXPECT_EQ(request.measurement, "sales");
    EXPECT_EQ(request.fields.size(), 1);
    EXPECT_EQ(request.fields[0], "amount");
    EXPECT_EQ(request.scopes.size(), 1);
    EXPECT_EQ(request.scopes.at("region"), "north");
    EXPECT_EQ(request.groupByTags.size(), 2);
    EXPECT_EQ(request.groupByTags[0], "product");
    EXPECT_EQ(request.groupByTags[1], "category");
}

// Test latest aggregation
TEST_F(QueryParserTest, ParseLatestQuery) {
    std::string query = "latest:system.metrics(cpu,memory){}";

    QueryRequest request = QueryParser::parseQueryString(query);

    EXPECT_EQ(request.aggregation, AggregationMethod::LATEST);
    EXPECT_EQ(request.measurement, "system.metrics");
    EXPECT_EQ(request.fields.size(), 2);
    EXPECT_TRUE(request.hasNoFilters());
}

// Test measurement with dots
TEST_F(QueryParserTest, ParseMeasurementWithDots) {
    std::string query = "avg:soil.moisture.sensor(value){}";

    QueryRequest request = QueryParser::parseQueryString(query);

    EXPECT_EQ(request.measurement, "soil.moisture.sensor");
    EXPECT_EQ(request.fields.size(), 1);
    EXPECT_EQ(request.fields[0], "value");
}

// Test whitespace handling
TEST_F(QueryParserTest, ParseQueryWithWhitespace) {
    std::string query = "  avg : temperature ( value , humidity ) { location : us-west } by { sensor }  ";

    QueryRequest request = QueryParser::parseQueryString(query);

    EXPECT_EQ(request.aggregation, AggregationMethod::AVG);
    EXPECT_EQ(request.measurement, "temperature");
    EXPECT_EQ(request.fields.size(), 2);
    EXPECT_EQ(request.fields[0], "value");
    EXPECT_EQ(request.fields[1], "humidity");
    EXPECT_EQ(request.scopes.at("location"), "us-west");
    EXPECT_EQ(request.groupByTags[0], "sensor");
}

// Test time parsing
TEST_F(QueryParserTest, ParseTimeFormat) {
    std::string timeStr = "15-03-2024 14:30:45";

    uint64_t timestamp = QueryParser::parseTime(timeStr);

    // Verify it's a reasonable timestamp (March 15, 2024)
    EXPECT_GT(timestamp, 1700000000000000000ULL);  // After Nov 2023
    EXPECT_LT(timestamp, 1800000000000000000ULL);  // Before 2027
}

// Test full query with time range
TEST_F(QueryParserTest, ParseFullQueryWithTimeRange) {
    std::string query = "avg:temperature(value){location:office}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "01-01-2024 23:59:59";

    QueryRequest request = QueryParser::parse(query, startTime, endTime);

    EXPECT_EQ(request.measurement, "temperature");
    EXPECT_LT(request.startTime, request.endTime);

    // Check that times are on the same day
    uint64_t timeDiff = request.endTime - request.startTime;
    uint64_t oneDayNanos = 24ULL * 60 * 60 * 1000000000;
    EXPECT_LT(timeDiff, oneDayNanos);
}

// Error cases
TEST_F(QueryParserTest, ErrorOnEmptyQuery) {
    EXPECT_THROW(QueryParser::parseQueryString(""), QueryParseException);
}

TEST_F(QueryParserTest, ErrorOnMissingColon) {
    EXPECT_THROW(QueryParser::parseQueryString("avg temperature()"), QueryParseException);
}

TEST_F(QueryParserTest, ErrorOnMissingMeasurement) {
    EXPECT_THROW(QueryParser::parseQueryString("avg:()"), QueryParseException);
}

TEST_F(QueryParserTest, ErrorOnInvalidAggregation) {
    EXPECT_THROW(QueryParser::parseQueryString("invalid:temperature()"), QueryParseException);
}

TEST_F(QueryParserTest, ErrorOnUnclosedParenthesis) {
    EXPECT_THROW(QueryParser::parseQueryString("avg:temperature(value"), QueryParseException);
}

TEST_F(QueryParserTest, ErrorOnUnclosedBrace) {
    EXPECT_THROW(QueryParser::parseQueryString("avg:temperature(){location:office"), QueryParseException);
}

TEST_F(QueryParserTest, ErrorOnInvalidScopeFormat) {
    EXPECT_THROW(QueryParser::parseQueryString("avg:temperature(){invalid_format}"), QueryParseException);
}

TEST_F(QueryParserTest, ErrorOnEmptyGroupBy) {
    EXPECT_THROW(QueryParser::parseQueryString("avg:temperature() by {}"), QueryParseException);
}

TEST_F(QueryParserTest, ErrorOnInvalidTimeFormat) {
    EXPECT_THROW(QueryParser::parseTime("2024-01-01 00:00:00"), QueryParseException);  // Wrong format
    EXPECT_THROW(QueryParser::parseTime("32-01-2024 00:00:00"), QueryParseException);  // Invalid day
    EXPECT_THROW(QueryParser::parseTime("01-13-2024 00:00:00"), QueryParseException);  // Invalid month
    EXPECT_THROW(QueryParser::parseTime("01/01/2024 00:00:00"), QueryParseException);  // Wrong delimiter
}

TEST_F(QueryParserTest, ErrorOnStartTimeAfterEndTime) {
    std::string query = "avg:temperature()";
    std::string startTime = "02-01-2024 00:00:00";
    std::string endTime = "01-01-2024 00:00:00";

    EXPECT_THROW(QueryParser::parse(query, startTime, endTime), QueryParseException);
}

// Complex query combinations
TEST_F(QueryParserTest, ParseComplexQuery1) {
    std::string query = "avg:sensor.data(temp,humidity,pressure){building:A,floor:3} by {room,sensor_id}";

    QueryRequest request = QueryParser::parseQueryString(query);

    EXPECT_EQ(request.aggregation, AggregationMethod::AVG);
    EXPECT_EQ(request.measurement, "sensor.data");
    EXPECT_EQ(request.fields.size(), 3);
    EXPECT_EQ(request.scopes.size(), 2);
    EXPECT_EQ(request.groupByTags.size(), 2);
}

TEST_F(QueryParserTest, ParseComplexQuery2) {
    // Query with everything optional omitted
    std::string query = "sum:transactions(){}";

    QueryRequest request = QueryParser::parseQueryString(query);

    EXPECT_EQ(request.aggregation, AggregationMethod::SUM);
    EXPECT_EQ(request.measurement, "transactions");
    EXPECT_TRUE(request.requestsAllFields());
    EXPECT_TRUE(request.hasNoFilters());
    EXPECT_FALSE(request.hasGroupBy());
}

// Case sensitivity tests
TEST_F(QueryParserTest, AggregationCaseInsensitive) {
    EXPECT_EQ(QueryParser::parseQueryString("AVG:temp()").aggregation, AggregationMethod::AVG);
    EXPECT_EQ(QueryParser::parseQueryString("Avg:temp()").aggregation, AggregationMethod::AVG);
    EXPECT_EQ(QueryParser::parseQueryString("avg:temp()").aggregation, AggregationMethod::AVG);
    EXPECT_EQ(QueryParser::parseQueryString("MIN:temp()").aggregation, AggregationMethod::MIN);
    EXPECT_EQ(QueryParser::parseQueryString("min:temp()").aggregation, AggregationMethod::MIN);
}

TEST_F(QueryParserTest, MeasurementCaseSensitive) {
    QueryRequest r1 = QueryParser::parseQueryString("avg:Temperature()");
    QueryRequest r2 = QueryParser::parseQueryString("avg:temperature()");

    EXPECT_EQ(r1.measurement, "Temperature");
    EXPECT_EQ(r2.measurement, "temperature");
    EXPECT_NE(r1.measurement, r2.measurement);
}

// Wildcard scope parsing — parser stores raw strings, wildcards pass through
TEST_F(QueryParserTest, ParseWildcardScope) {
    QueryRequest request = QueryParser::parseQueryString("avg:cpu(usage){host:server-*}");

    EXPECT_EQ(request.scopes.size(), 1);
    EXPECT_EQ(request.scopes.at("host"), "server-*");
    EXPECT_TRUE(request.hasPatternFilters());
}

// ~regex scope parsing
TEST_F(QueryParserTest, ParseTildeRegexScope) {
    QueryRequest request = QueryParser::parseQueryString("avg:cpu(usage){host:~server-[0-9]+}");

    EXPECT_EQ(request.scopes.size(), 1);
    EXPECT_EQ(request.scopes.at("host"), "~server-[0-9]+");
    EXPECT_TRUE(request.hasPatternFilters());
}

// /regex/ scope parsing
TEST_F(QueryParserTest, ParseSlashRegexScope) {
    // Note: /regex/ containing } would break the parser, but simple patterns work
    QueryRequest request = QueryParser::parseQueryString("avg:cpu(usage){host:/server-[0-9]+/}");

    EXPECT_EQ(request.scopes.size(), 1);
    EXPECT_EQ(request.scopes.at("host"), "/server-[0-9]+/");
    EXPECT_TRUE(request.hasPatternFilters());
}

// Exact scope returns false for hasPatternFilters
TEST_F(QueryParserTest, ExactScopeNotPattern) {
    QueryRequest request = QueryParser::parseQueryString("avg:cpu(usage){host:server-01}");

    EXPECT_EQ(request.scopes.at("host"), "server-01");
    EXPECT_FALSE(request.hasPatternFilters());
}

// Mixed exact and pattern scopes
TEST_F(QueryParserTest, MixedExactAndPatternScopes) {
    QueryRequest request = QueryParser::parseQueryString("avg:cpu(usage){host:server-*,dc:dc1}");

    EXPECT_EQ(request.scopes.at("host"), "server-*");
    EXPECT_EQ(request.scopes.at("dc"), "dc1");
    EXPECT_TRUE(request.hasPatternFilters());
}