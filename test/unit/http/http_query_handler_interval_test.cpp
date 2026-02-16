#include <gtest/gtest.h>
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/query/query_parser.hpp"
#include <cstdint>
#include <limits>

using namespace tsdb;

class HttpQueryHandlerIntervalTest : public ::testing::Test {};

// ---- Normal intervals with units ----

TEST_F(HttpQueryHandlerIntervalTest, ParseSeconds) {
    EXPECT_EQ(HttpQueryHandler::parseInterval("30s"), 30ULL * 1000000000);
}

TEST_F(HttpQueryHandlerIntervalTest, ParseMinutes) {
    EXPECT_EQ(HttpQueryHandler::parseInterval("5m"), 5ULL * 60 * 1000000000);
}

TEST_F(HttpQueryHandlerIntervalTest, ParseHours) {
    EXPECT_EQ(HttpQueryHandler::parseInterval("1h"), 3600ULL * 1000000000);
}

TEST_F(HttpQueryHandlerIntervalTest, ParseDays) {
    EXPECT_EQ(HttpQueryHandler::parseInterval("1d"), 86400ULL * 1000000000);
}

TEST_F(HttpQueryHandlerIntervalTest, ParseMilliseconds) {
    EXPECT_EQ(HttpQueryHandler::parseInterval("100ms"), 100ULL * 1000000);
}

TEST_F(HttpQueryHandlerIntervalTest, ParseNanoseconds) {
    EXPECT_EQ(HttpQueryHandler::parseInterval("500ns"), 500ULL);
}

TEST_F(HttpQueryHandlerIntervalTest, ParseMicroseconds) {
    EXPECT_EQ(HttpQueryHandler::parseInterval("250us"), 250ULL * 1000);
}

TEST_F(HttpQueryHandlerIntervalTest, ParseMicrosecondsUnicode) {
    // The µs unit (micro sign)
    EXPECT_EQ(HttpQueryHandler::parseInterval("250µs"), 250ULL * 1000);
}

// ---- Decimal intervals ----

TEST_F(HttpQueryHandlerIntervalTest, ParseDecimalSeconds) {
    EXPECT_EQ(HttpQueryHandler::parseInterval("1.5s"), 1500000000ULL);
}

TEST_F(HttpQueryHandlerIntervalTest, ParseDecimalMinutes) {
    EXPECT_EQ(HttpQueryHandler::parseInterval("0.5m"), 30ULL * 1000000000);
}

// ---- Bare numbers (nanoseconds) ----

TEST_F(HttpQueryHandlerIntervalTest, BareNumberTreatedAsNanoseconds) {
    EXPECT_EQ(HttpQueryHandler::parseInterval("300000000000"), 300000000000ULL);
}

TEST_F(HttpQueryHandlerIntervalTest, BareNumberSmall) {
    EXPECT_EQ(HttpQueryHandler::parseInterval("1"), 1ULL);
}

TEST_F(HttpQueryHandlerIntervalTest, BareNumberUint64Max) {
    // UINT64_MAX = 18446744073709551615
    EXPECT_EQ(HttpQueryHandler::parseInterval("18446744073709551615"),
              std::numeric_limits<uint64_t>::max());
}

// ---- Overflow detection ----

TEST_F(HttpQueryHandlerIntervalTest, OverflowDaysThrows) {
    // 99999999999 days in nanoseconds would overflow uint64_t
    EXPECT_THROW(HttpQueryHandler::parseInterval("99999999999d"), QueryParseException);
}

TEST_F(HttpQueryHandlerIntervalTest, OverflowHoursThrows) {
    // A very large hour value that overflows
    EXPECT_THROW(HttpQueryHandler::parseInterval("99999999999h"), QueryParseException);
}

TEST_F(HttpQueryHandlerIntervalTest, OverflowDecimalThrows) {
    // Very large decimal that overflows
    EXPECT_THROW(HttpQueryHandler::parseInterval("999999999999999.0d"), QueryParseException);
}

// ---- Boundary: largest non-overflowing values ----

TEST_F(HttpQueryHandlerIntervalTest, LargestNonOverflowingSeconds) {
    // UINT64_MAX / 1e9 = 18446744073 seconds (approx)
    // 18446744073s should NOT overflow
    uint64_t result = HttpQueryHandler::parseInterval("18446744073s");
    EXPECT_GT(result, 0ULL);
}

TEST_F(HttpQueryHandlerIntervalTest, SmallestOverflowingSeconds) {
    // 18446744074s would overflow (18446744074 * 1e9 > UINT64_MAX)
    EXPECT_THROW(HttpQueryHandler::parseInterval("18446744074s"), QueryParseException);
}

// ---- Invalid formats ----

TEST_F(HttpQueryHandlerIntervalTest, EmptyStringThrows) {
    EXPECT_THROW(HttpQueryHandler::parseInterval(""), QueryParseException);
}

TEST_F(HttpQueryHandlerIntervalTest, PureTextThrows) {
    EXPECT_THROW(HttpQueryHandler::parseInterval("abc"), QueryParseException);
}

TEST_F(HttpQueryHandlerIntervalTest, UnknownUnitThrows) {
    EXPECT_THROW(HttpQueryHandler::parseInterval("5x"), QueryParseException);
}

TEST_F(HttpQueryHandlerIntervalTest, UnknownUnitErrorMessage) {
    try {
        HttpQueryHandler::parseInterval("5x");
        FAIL() << "Expected QueryParseException";
    } catch (const QueryParseException& e) {
        std::string msg = e.what();
        EXPECT_NE(msg.find("Unknown time unit"), std::string::npos)
            << "Error message should mention unknown time unit, got: " << msg;
    }
}
