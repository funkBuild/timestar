#include <gtest/gtest.h>
#include "../../../lib/query/query_parser.hpp"

using namespace tsdb;

class QueryParserTimezoneTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// Test that parseTime interprets time strings as UTC, not local time.
// Jan 1, 2024 00:00:00 UTC = 1704067200 seconds since epoch = 1704067200000000000 nanoseconds
TEST_F(QueryParserTimezoneTest, ParseTimeReturnsUTCTimestamp) {
    std::string timeStr = "01-01-2024 00:00:00";
    uint64_t timestamp = QueryParser::parseTime(timeStr);
    EXPECT_EQ(timestamp, 1704067200000000000ULL);
}

// Test another known UTC timestamp.
// June 15, 2023 08:45:00 UTC = 1686818700 seconds since epoch = 1686818700000000000 nanoseconds
TEST_F(QueryParserTimezoneTest, ParseTimeReturnsUTCTimestampJune) {
    std::string timeStr = "15-06-2023 08:45:00";
    uint64_t timestamp = QueryParser::parseTime(timeStr);
    EXPECT_EQ(timestamp, 1686818700000000000ULL);
}
