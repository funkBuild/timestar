#include <gtest/gtest.h>
#include "../../../lib/query/query_parser.hpp"
#include <iostream>

using namespace tsdb;

class QueryParserDetailedTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Setup code if needed
    }
    
    void TearDown() override {
        // Cleanup code if needed
    }
};

// ============================================================================
// Basic Query Format Tests
// ============================================================================

TEST_F(QueryParserDetailedTest, BasicQueryWithEmptyScopes) {
    std::string query = "avg:temperature(value){}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.aggregation, AggregationMethod::AVG);
        EXPECT_EQ(request.measurement, "temperature");
        EXPECT_EQ(request.fields.size(), 1);
        EXPECT_EQ(request.fields[0], "value");
        EXPECT_TRUE(request.scopes.empty());
    });
}

TEST_F(QueryParserDetailedTest, QueryWithoutScopes) {
    std::string query = "avg:temperature(value)";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    // This should handle missing scopes gracefully
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.aggregation, AggregationMethod::AVG);
        EXPECT_EQ(request.measurement, "temperature");
        EXPECT_TRUE(request.scopes.empty());
    });
}

TEST_F(QueryParserDetailedTest, QueryWithEmptyFields) {
    std::string query = "avg:temperature(){}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_TRUE(request.fields.empty());
    });
}

// ============================================================================
// Scope Tests with Special Characters
// ============================================================================

TEST_F(QueryParserDetailedTest, ScopeWithSimpleValue) {
    std::string query = "avg:temperature(value){host:server1}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.scopes.size(), 1);
        EXPECT_EQ(request.scopes["host"], "server1");
    });
}

TEST_F(QueryParserDetailedTest, ScopeWithHyphen) {
    std::string query = "avg:temperature(value){host:server-01}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.scopes.size(), 1);
        EXPECT_EQ(request.scopes["host"], "server-01");
    });
}

TEST_F(QueryParserDetailedTest, ScopeWithUnderscore) {
    std::string query = "avg:temperature(value){host:server_01}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.scopes.size(), 1);
        EXPECT_EQ(request.scopes["host"], "server_01");
    });
}

TEST_F(QueryParserDetailedTest, ScopeWithDot) {
    std::string query = "avg:temperature(value){host:server.prod.01}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.scopes.size(), 1);
        EXPECT_EQ(request.scopes["host"], "server.prod.01");
    });
}

TEST_F(QueryParserDetailedTest, ScopeWithNumbers) {
    std::string query = "avg:temperature(value){host:server123}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.scopes.size(), 1);
        EXPECT_EQ(request.scopes["host"], "server123");
    });
}

TEST_F(QueryParserDetailedTest, MultipleScopes) {
    std::string query = "avg:temperature(value){host:server-01,region:us-west}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.scopes.size(), 2);
        EXPECT_EQ(request.scopes["host"], "server-01");
        EXPECT_EQ(request.scopes["region"], "us-west");
    });
}

TEST_F(QueryParserDetailedTest, ComplexScopeValues) {
    std::string query = "avg:temperature(value){host:prod-server-01.example.com,env:staging_v2}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.scopes.size(), 2);
        EXPECT_EQ(request.scopes["host"], "prod-server-01.example.com");
        EXPECT_EQ(request.scopes["env"], "staging_v2");
    });
}

// ============================================================================
// Multiple Fields Tests
// ============================================================================

TEST_F(QueryParserDetailedTest, MultipleFields) {
    std::string query = "avg:temperature(value,humidity,pressure){}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.fields.size(), 3);
        EXPECT_EQ(request.fields[0], "value");
        EXPECT_EQ(request.fields[1], "humidity");
        EXPECT_EQ(request.fields[2], "pressure");
    });
}

TEST_F(QueryParserDetailedTest, FieldsWithSpaces) {
    std::string query = "avg:temperature(value, humidity, pressure){}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.fields.size(), 3);
        // Should trim spaces
        EXPECT_EQ(request.fields[0], "value");
        EXPECT_EQ(request.fields[1], "humidity");
        EXPECT_EQ(request.fields[2], "pressure");
    });
}

// ============================================================================
// Different Aggregation Types
// ============================================================================

TEST_F(QueryParserDetailedTest, MinAggregation) {
    std::string query = "min:temperature(value){}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.aggregation, AggregationMethod::MIN);
    });
}

TEST_F(QueryParserDetailedTest, MaxAggregation) {
    std::string query = "max:temperature(value){}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.aggregation, AggregationMethod::MAX);
    });
}

TEST_F(QueryParserDetailedTest, SumAggregation) {
    std::string query = "sum:temperature(value){}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.aggregation, AggregationMethod::SUM);
    });
}

TEST_F(QueryParserDetailedTest, LatestAggregation) {
    std::string query = "latest:temperature(value){}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.aggregation, AggregationMethod::LATEST);
    });
}

// ============================================================================
// Measurement Name Tests
// ============================================================================

TEST_F(QueryParserDetailedTest, MeasurementWithUnderscore) {
    std::string query = "avg:cpu_usage(value){}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.measurement, "cpu_usage");
    });
}

TEST_F(QueryParserDetailedTest, MeasurementWithDot) {
    std::string query = "avg:system.cpu.usage(value){}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.measurement, "system.cpu.usage");
    });
}

TEST_F(QueryParserDetailedTest, MeasurementWithHyphen) {
    std::string query = "avg:cpu-usage(value){}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.measurement, "cpu-usage");
    });
}

// ============================================================================
// Time Format Tests
// ============================================================================

TEST_F(QueryParserDetailedTest, TimeFormatWithSingleDigitDay) {
    std::string query = "avg:temperature(value){}";
    std::string startTime = "1-1-2024 00:00:00";
    std::string endTime = "9-1-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_GT(request.startTime, 0);
        EXPECT_GT(request.endTime, request.startTime);
    });
}

TEST_F(QueryParserDetailedTest, TimeFormatWithSingleDigitMonth) {
    std::string query = "avg:temperature(value){}";
    std::string startTime = "01-1-2024 00:00:00";
    std::string endTime = "31-1-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_GT(request.startTime, 0);
        EXPECT_GT(request.endTime, request.startTime);
    });
}

TEST_F(QueryParserDetailedTest, TimeFormatWithMidnight) {
    std::string query = "avg:temperature(value){}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "01-01-2024 00:00:01";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.endTime - request.startTime, 1000000000); // 1 second in nanoseconds
    });
}

TEST_F(QueryParserDetailedTest, TimeFormatWithNoon) {
    std::string query = "avg:temperature(value){}";
    std::string startTime = "01-01-2024 12:00:00";
    std::string endTime = "01-01-2024 12:00:01";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.endTime - request.startTime, 1000000000); // 1 second in nanoseconds
    });
}

TEST_F(QueryParserDetailedTest, TimeFormatEndOfDay) {
    std::string query = "avg:temperature(value){}";
    std::string startTime = "01-01-2024 23:59:58";
    std::string endTime = "01-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.endTime - request.startTime, 1000000000); // 1 second in nanoseconds
    });
}

// ============================================================================
// Error Cases
// ============================================================================

TEST_F(QueryParserDetailedTest, InvalidQueryFormatNoColon) {
    std::string query = "avgtemperature(value){}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    EXPECT_THROW({
        QueryParser::parse(query, startTime, endTime);
    }, QueryParseException);
}

TEST_F(QueryParserDetailedTest, InvalidScopeFormatNoColon) {
    std::string query = "avg:temperature(value){hostserver1}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    EXPECT_THROW({
        QueryParser::parse(query, startTime, endTime);
    }, QueryParseException);
}

TEST_F(QueryParserDetailedTest, EmptyScopeKey) {
    std::string query = "avg:temperature(value){:value1}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    EXPECT_THROW({
        QueryParser::parse(query, startTime, endTime);
    }, QueryParseException);
}

TEST_F(QueryParserDetailedTest, EmptyScopeValue) {
    std::string query = "avg:temperature(value){host:}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    EXPECT_THROW({
        QueryParser::parse(query, startTime, endTime);
    }, QueryParseException);
}

TEST_F(QueryParserDetailedTest, InvalidTimeFormat) {
    std::string query = "avg:temperature(value){}";
    std::string startTime = "2024-01-01 00:00:00"; // Wrong format (should be dd-mm-yyyy)
    std::string endTime = "2024-01-31 23:59:59";
    
    EXPECT_THROW({
        QueryParser::parse(query, startTime, endTime);
    }, QueryParseException);
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST_F(QueryParserDetailedTest, ExtraSpacesInQuery) {
    std::string query = "  avg : temperature ( value , humidity ) { host : server1 , region : west }  ";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.aggregation, AggregationMethod::AVG);
        EXPECT_EQ(request.measurement, "temperature");
        EXPECT_EQ(request.fields.size(), 2);
        EXPECT_EQ(request.scopes.size(), 2);
    });
}

TEST_F(QueryParserDetailedTest, VeryLongTagValue) {
    std::string longValue = "this-is-a-very-long-tag-value-with-many-characters-123456789";
    std::string query = "avg:temperature(value){host:" + longValue + "}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.scopes["host"], longValue);
    });
}

TEST_F(QueryParserDetailedTest, SpecialCharactersInFieldNames) {
    std::string query = "avg:temperature(cpu_usage,memory.used,disk-io){}";
    std::string startTime = "01-01-2024 00:00:00";
    std::string endTime = "31-01-2024 23:59:59";
    
    ASSERT_NO_THROW({
        auto request = QueryParser::parse(query, startTime, endTime);
        EXPECT_EQ(request.fields.size(), 3);
        EXPECT_EQ(request.fields[0], "cpu_usage");
        EXPECT_EQ(request.fields[1], "memory.used");
        EXPECT_EQ(request.fields[2], "disk-io");
    });
}

// Main function for running tests
int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}