#include "../../../lib/http/response_formatter.hpp"
#include "../../../lib/http/http_query_handler.hpp"

#include <glaze/glaze.hpp>

#include <gtest/gtest.h>

#include <string>

using namespace timestar;

// Glaze structure for parsing error responses
struct ErrorResponseParsed {
    std::string status;
    std::string message;
    std::string error;
};

template <>
struct glz::meta<ErrorResponseParsed> {
    using T = ErrorResponseParsed;
    static constexpr auto value = object("status", &T::status, "message", &T::message, "error", &T::error);
};

// Glaze structure for parsing success responses (allow unknown keys)
struct SuccessResponseParsed {
    std::string status;
};

template <>
struct glz::meta<SuccessResponseParsed> {
    using T = SuccessResponseParsed;
    static constexpr auto value = object("status", &T::status);
};

// Read options that skip unknown keys (success responses have series, statistics, etc.)
static constexpr glz::opts lenient_opts{.error_on_unknown_keys = false};

class HttpErrorResponseTest : public ::testing::Test {};

// --- ResponseFormatter::formatError() tests ---

TEST_F(HttpErrorResponseTest, ResponseFormatterErrorHasStatusField) {
    std::string json = ResponseFormatter::formatError("something went wrong");
    EXPECT_NE(json.find("\"status\":\"error\""), std::string::npos)
        << "formatError() output must contain \"status\":\"error\", got: " << json;
}

TEST_F(HttpErrorResponseTest, ResponseFormatterErrorHasMessageField) {
    std::string msg = "disk full on shard 3";
    std::string json = ResponseFormatter::formatError(msg);

    ErrorResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "Failed to parse formatError() output as JSON: " << json;
    EXPECT_EQ(parsed.message, msg);
}

TEST_F(HttpErrorResponseTest, ResponseFormatterErrorIsValidJson) {
    std::string json = ResponseFormatter::formatError("test error");

    ErrorResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "formatError() output is not valid JSON: " << json;
    EXPECT_EQ(parsed.status, "error");
    EXPECT_EQ(parsed.message, "test error");
    EXPECT_EQ(parsed.error, "test error");
}

TEST_F(HttpErrorResponseTest, ErrorMessageWithQuotesIsEscaped) {
    std::string msg = "field \"value\" is invalid";
    std::string json = ResponseFormatter::formatError(msg);

    // Must be valid JSON even with embedded quotes
    ErrorResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "JSON with escaped quotes failed to parse: " << json;
    EXPECT_EQ(parsed.message, msg);
    EXPECT_EQ(parsed.error, msg);
}

TEST_F(HttpErrorResponseTest, ErrorMessageWithBackslashIsEscaped) {
    std::string msg = "path C:\\data\\tsm not found";
    std::string json = ResponseFormatter::formatError(msg);

    ErrorResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "JSON with escaped backslashes failed to parse: " << json;
    EXPECT_EQ(parsed.message, msg);
    EXPECT_EQ(parsed.error, msg);
}

TEST_F(HttpErrorResponseTest, ErrorMessageWithNewlineIsEscaped) {
    std::string msg = "line one\nline two";
    std::string json = ResponseFormatter::formatError(msg);

    // Raw newline must not appear in JSON output
    EXPECT_EQ(json.find('\n'), std::string::npos)
        << "Raw newline character must be escaped in JSON output";

    ErrorResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "JSON with escaped newline failed to parse: " << json;
    EXPECT_EQ(parsed.message, msg);
}

TEST_F(HttpErrorResponseTest, ErrorMessageWithUnicodeIsPreserved) {
    // UTF-8 string with various scripts
    std::string msg = "Fehler: Messung nicht gefunden";
    std::string json = ResponseFormatter::formatError(msg);

    ErrorResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "JSON with UTF-8 message failed to parse: " << json;
    EXPECT_EQ(parsed.message, msg);
}

TEST_F(HttpErrorResponseTest, EmptyErrorMessage) {
    std::string json = ResponseFormatter::formatError("");

    ErrorResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "formatError(\"\") must produce valid JSON: " << json;
    EXPECT_EQ(parsed.status, "error");
    EXPECT_EQ(parsed.message, "");
    EXPECT_EQ(parsed.error, "");
}

TEST_F(HttpErrorResponseTest, VeryLongErrorMessage) {
    // 10 KB error message
    std::string msg(10240, 'x');
    std::string json = ResponseFormatter::formatError(msg);

    ErrorResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "formatError() with 10KB message must produce valid JSON";
    EXPECT_EQ(parsed.status, "error");
    EXPECT_EQ(parsed.message, msg);
    EXPECT_EQ(parsed.error, msg);
}

TEST_F(HttpErrorResponseTest, QueryHandlerErrorResponse) {
    // HttpQueryHandler::createErrorResponse delegates to ResponseFormatter::formatError,
    // but verify the handler can be constructed with nullptr and called.
    HttpQueryHandler handler(nullptr, nullptr);
    std::string json = handler.createErrorResponse("INVALID_QUERY", "missing measurement");

    ErrorResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "HttpQueryHandler::createErrorResponse() must produce valid JSON: " << json;
    EXPECT_EQ(parsed.status, "error");
    // The message field should contain the message argument
    EXPECT_EQ(parsed.message, "missing measurement");
}

TEST_F(HttpErrorResponseTest, SuccessResponseHasStatusSuccess) {
    // Build a minimal QueryResponse and format it
    QueryResponse response;
    response.success = true;
    response.statistics.seriesCount = 0;
    response.statistics.pointCount = 0;
    response.statistics.executionTimeMs = 0.0;

    std::string json = ResponseFormatter::format(response);

    EXPECT_NE(json.find("\"status\":\"success\""), std::string::npos)
        << "format() output must contain \"status\":\"success\", got: " << json;

    // Also verify it parses (use lenient opts since success responses have extra fields)
    SuccessResponseParsed parsed;
    auto err = glz::read<lenient_opts>(parsed, json);
    ASSERT_FALSE(bool(err)) << "format() output is not valid JSON: " << json;
    EXPECT_EQ(parsed.status, "success");
}
