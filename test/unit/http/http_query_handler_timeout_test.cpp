#include "../../../lib/http/http_query_handler.hpp"

#include <glaze/glaze.hpp>

#include <gtest/gtest.h>

#include <chrono>

using namespace timestar;

// Glaze structures for parsing error responses
struct TimeoutTestErrorResponse {
    std::string status;
    std::string message;
    std::string error;
};

template <>
struct glz::meta<TimeoutTestErrorResponse> {
    using T = TimeoutTestErrorResponse;
    static constexpr auto value = object("status", &T::status, "message", &T::message, "error", &T::error);
};

class HttpQueryHandlerTimeoutTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// =============================================================================
// defaultQueryTimeout() Tests
// Verify the timeout function exists and has a reasonable default value
// =============================================================================

TEST_F(HttpQueryHandlerTimeoutTest, DefaultQueryTimeoutExists) {
    auto timeout = HttpQueryHandler::defaultQueryTimeout();
    EXPECT_GT(timeout.count(), 0) << "defaultQueryTimeout() should return a positive duration";
}

TEST_F(HttpQueryHandlerTimeoutTest, DefaultQueryTimeoutIs30Seconds) {
    EXPECT_EQ(HttpQueryHandler::defaultQueryTimeout().count(), 30) << "Default query timeout should be 30 seconds";
}

TEST_F(HttpQueryHandlerTimeoutTest, DefaultQueryTimeoutIsPositive) {
    EXPECT_GT(HttpQueryHandler::defaultQueryTimeout().count(), 0) << "Query timeout must be a positive duration";
}

TEST_F(HttpQueryHandlerTimeoutTest, DefaultQueryTimeoutIsReasonable) {
    // Timeout should be between 5 seconds and 5 minutes for a query
    auto timeoutSeconds = HttpQueryHandler::defaultQueryTimeout().count();
    EXPECT_GE(timeoutSeconds, 5) << "Query timeout should be at least 5 seconds";
    EXPECT_LE(timeoutSeconds, 300) << "Query timeout should not exceed 5 minutes";
}

TEST_F(HttpQueryHandlerTimeoutTest, DefaultQueryTimeoutTypeIsChronoSeconds) {
    // Verify the return type is std::chrono::seconds
    static_assert(std::is_same_v<decltype(HttpQueryHandler::defaultQueryTimeout()), std::chrono::seconds>,
                  "defaultQueryTimeout() should return std::chrono::seconds");
    SUCCEED();
}

// =============================================================================
// Error Response Format Tests for Timeout Errors
// Verify that createErrorResponse produces valid JSON for timeout scenarios
// =============================================================================

TEST_F(HttpQueryHandlerTimeoutTest, CreateTimeoutErrorResponse) {
    HttpQueryHandler handler(nullptr);

    std::string json = handler.createErrorResponse("QUERY_TIMEOUT", "Query timed out after 30 seconds");

    // Parse and verify the JSON structure
    TimeoutTestErrorResponse parsed;
    auto error = glz::read_json(parsed, json);

    ASSERT_FALSE(error) << "JSON parse error: " << glz::format_error(error);
    EXPECT_EQ(parsed.status, "error");
    EXPECT_EQ(parsed.message, "Query timed out after 30 seconds");
    EXPECT_EQ(parsed.error, "Query timed out after 30 seconds");
}

TEST_F(HttpQueryHandlerTimeoutTest, TimeoutErrorMessageContainsTimeoutDuration) {
    HttpQueryHandler handler(nullptr);

    // Simulate the exact error message the timeout handler would generate
    std::string timeoutMsg =
        "Query timed out after " + std::to_string(HttpQueryHandler::defaultQueryTimeout().count()) + " seconds";

    std::string json = handler.createErrorResponse("QUERY_TIMEOUT", timeoutMsg);

    TimeoutTestErrorResponse parsed;
    auto error = glz::read_json(parsed, json);

    ASSERT_FALSE(error) << "JSON parse error: " << glz::format_error(error);
    EXPECT_NE(parsed.message.find("30"), std::string::npos)
        << "Timeout error message should contain the timeout duration";
    EXPECT_NE(parsed.message.find("seconds"), std::string::npos) << "Timeout error message should mention seconds";
}

TEST_F(HttpQueryHandlerTimeoutTest, TimeoutErrorResponseIsValidJson) {
    HttpQueryHandler handler(nullptr);

    std::string json = handler.createErrorResponse("QUERY_TIMEOUT", "Query timed out after 30 seconds");

    // Verify the JSON is well-formed by checking it can be parsed
    TimeoutTestErrorResponse parsed;
    auto error = glz::read_json(parsed, json);
    ASSERT_FALSE(error) << "Timeout error response must be valid JSON: " << glz::format_error(error);
}
