#include <gtest/gtest.h>

#include <fstream>
#include <string>

// =============================================================================
// Source inspection tests: verify that every co_await engineSharded->invoke_on()
// call in HttpMetadataHandler is enclosed in a try/catch block.
//
// Without the fix, an engine exception (e.g. LevelDB I/O error) would propagate
// out of the coroutine as an unhandled exception rather than being caught and
// turned into an HTTP 500 response.
//
// The test reads the actual source file at test-time and checks structural
// invariants using string search.  This approach is used elsewhere in the test
// suite (see query_runner_race_test.cpp) to lock in properties that cannot be
// verified through a normal unit test because Seastar is hard to mock.
// =============================================================================

class MetadataHandlerExceptionSafetyTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
        std::ifstream file(HTTP_METADATA_HANDLER_SOURCE_PATH);
        ASSERT_TRUE(file.is_open()) << "Could not open http_metadata_handler.cpp at: "
                                    << HTTP_METADATA_HANDLER_SOURCE_PATH;
        sourceCode.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
        ASSERT_FALSE(sourceCode.empty()) << "http_metadata_handler.cpp is unexpectedly empty";
    }

    // Extract the body of a named member function by brace-matching.
    // Starts scanning from the LAST '{' that follows `funcSignatureSubstr`
    // where the signature must also contain the class-qualified form
    // (e.g. "HttpMetadataHandler::handleMeasurements") to avoid matching
    // call sites that appear earlier in the file.
    // Returns an empty string when the function cannot be found.
    std::string extractFunctionBody(const std::string& funcSignatureSubstr) const {
        // Find the last occurrence of the qualified definition, not a call site.
        // We scan for all occurrences and pick the one followed by '(' to locate
        // the function definition (which always has parameters).
        size_t sigPos = std::string::npos;
        size_t searchFrom = 0;
        while (true) {
            size_t found = sourceCode.find(funcSignatureSubstr, searchFrom);
            if (found == std::string::npos)
                break;
            sigPos = found;
            searchFrom = found + 1;
        }
        if (sigPos == std::string::npos) {
            return "";
        }
        // Find the opening brace of the function body (skip the parameter list)
        // by finding the first '{' after the closing ')' of the parameter list.
        auto closeParen = sourceCode.find(')', sigPos);
        if (closeParen == std::string::npos) {
            return "";
        }
        auto openBrace = sourceCode.find('{', closeParen);
        if (openBrace == std::string::npos) {
            return "";
        }
        // Brace-match to find the closing brace
        int depth = 1;
        size_t pos = openBrace + 1;
        while (pos < sourceCode.size() && depth > 0) {
            if (sourceCode[pos] == '{')
                depth++;
            else if (sourceCode[pos] == '}')
                depth--;
            pos++;
        }
        if (depth != 0) {
            return "";  // Unmatched braces
        }
        return sourceCode.substr(openBrace, pos - openBrace);
    }
};

// ---------------------------------------------------------------------------
// handleMeasurements: the co_await engineSharded->invoke_on() call must be
// inside a try block.
// ---------------------------------------------------------------------------
TEST_F(MetadataHandlerExceptionSafetyTest, HandleMeasurementsTryCatchBeforeCoAwait) {
    std::string body = extractFunctionBody("HttpMetadataHandler::handleMeasurements");
    ASSERT_FALSE(body.empty())
        << "Could not locate HttpMetadataHandler::handleMeasurements in http_metadata_handler.cpp";

    // The function must contain a co_await engineSharded call
    auto coAwaitPos = body.find("co_await engineSharded");
    ASSERT_NE(coAwaitPos, std::string::npos) << "handleMeasurements does not contain 'co_await engineSharded'. "
                                             << "If the engine call was removed, update this test.";

    // A 'try {' must appear BEFORE the first co_await engineSharded
    auto tryPos = body.find("try {");
    ASSERT_NE(tryPos, std::string::npos) << "handleMeasurements has no 'try {' block. "
                                         << "Engine exceptions would propagate as unhandled coroutine exceptions "
                                         << "instead of being returned as HTTP 500 responses.";

    EXPECT_LT(tryPos, coAwaitPos) << "In handleMeasurements, 'try {' must appear before 'co_await engineSharded'. "
                                  << "Without this, an engine I/O error escapes the coroutine unhandled.";

    // A matching catch block for std::exception must exist
    EXPECT_NE(body.find("catch (const std::exception&"), std::string::npos)
        << "handleMeasurements is missing 'catch (const std::exception&'. "
        << "The try block must have a catch that converts exceptions to HTTP 500.";
}

// ---------------------------------------------------------------------------
// handleTags: the co_await engineSharded->invoke_on() call must be inside
// a try block.
// ---------------------------------------------------------------------------
TEST_F(MetadataHandlerExceptionSafetyTest, HandleTagsTryCatchBeforeCoAwait) {
    std::string body = extractFunctionBody("HttpMetadataHandler::handleTags");
    ASSERT_FALSE(body.empty()) << "Could not locate HttpMetadataHandler::handleTags in http_metadata_handler.cpp";

    auto coAwaitPos = body.find("co_await engineSharded");
    ASSERT_NE(coAwaitPos, std::string::npos) << "handleTags does not contain 'co_await engineSharded'. "
                                             << "If the engine call was removed, update this test.";

    auto tryPos = body.find("try {");
    ASSERT_NE(tryPos, std::string::npos) << "handleTags has no 'try {' block. "
                                         << "Engine exceptions would propagate as unhandled coroutine exceptions "
                                         << "instead of being returned as HTTP 500 responses.";

    EXPECT_LT(tryPos, coAwaitPos) << "In handleTags, 'try {' must appear before 'co_await engineSharded'. "
                                  << "Without this, an engine I/O error (e.g. LevelDB read failure) "
                                  << "escapes the coroutine unhandled.";

    EXPECT_NE(body.find("catch (const std::exception&"), std::string::npos)
        << "handleTags is missing 'catch (const std::exception&'. "
        << "The try block must have a catch that converts exceptions to HTTP 500.";
}

// ---------------------------------------------------------------------------
// handleFields: there are two co_await engineSharded->invoke_on() calls
// (getMeasurementFields and getFieldType). Both must be covered by a try block.
// ---------------------------------------------------------------------------
TEST_F(MetadataHandlerExceptionSafetyTest, HandleFieldsTryCatchBeforeCoAwait) {
    std::string body = extractFunctionBody("HttpMetadataHandler::handleFields");
    ASSERT_FALSE(body.empty()) << "Could not locate HttpMetadataHandler::handleFields in http_metadata_handler.cpp";

    auto coAwaitPos = body.find("co_await engineSharded");
    ASSERT_NE(coAwaitPos, std::string::npos) << "handleFields does not contain 'co_await engineSharded'. "
                                             << "If the engine call was removed, update this test.";

    auto tryPos = body.find("try {");
    ASSERT_NE(tryPos, std::string::npos) << "handleFields has no 'try {' block. "
                                         << "Engine exceptions would propagate as unhandled coroutine exceptions "
                                         << "instead of being returned as HTTP 500 responses.";

    EXPECT_LT(tryPos, coAwaitPos) << "In handleFields, 'try {' must appear before 'co_await engineSharded'. "
                                  << "Without this, an engine I/O error (e.g. LevelDB read failure during "
                                  << "getMeasurementFields or getFieldType) escapes unhandled.";

    EXPECT_NE(body.find("catch (const std::exception&"), std::string::npos)
        << "handleFields is missing 'catch (const std::exception&'. "
        << "The try block must have a catch that converts exceptions to HTTP 500.";

    // handleFields calls engineSharded->invoke_on() twice (getMeasurementFields
    // and getFieldType in the per-field loop).  Both are inside the same outer
    // try block, so a single 'try {' before the first call is sufficient.
    // Verify the second call exists as well.
    auto secondCoAwaitPos = body.find("co_await engineSharded", coAwaitPos + 1);
    EXPECT_NE(secondCoAwaitPos, std::string::npos)
        << "handleFields is expected to have two co_await engineSharded calls "
        << "(getMeasurementFields and getFieldType). "
        << "If the implementation changed, update this test accordingly.";
}

// ---------------------------------------------------------------------------
// Verify that all three handlers set HTTP 500 status when returning an error
// response (not 200 with an error body, which would be misleading).
// ---------------------------------------------------------------------------
TEST_F(MetadataHandlerExceptionSafetyTest, ErrorPathSetsHttp500Status) {
    // Each handler's catch block should set internal_server_error status
    size_t count = 0;
    size_t pos = 0;
    const std::string pattern = "internal_server_error";
    while ((pos = sourceCode.find(pattern, pos)) != std::string::npos) {
        count++;
        pos++;
    }
    EXPECT_GE(count, 3u) << "Expected at least 3 occurrences of 'internal_server_error' in "
                         << "http_metadata_handler.cpp (one per handler catch block). "
                         << "Found: " << count << ". "
                         << "Each handler's catch block must set the reply status to HTTP 500.";
}

// ---------------------------------------------------------------------------
// Verify that the error catch blocks call createErrorResponse() to produce
// a well-formed JSON body (not a raw exception message string).
// ---------------------------------------------------------------------------
TEST_F(MetadataHandlerExceptionSafetyTest, ErrorPathUsesCreateErrorResponse) {
    size_t count = 0;
    size_t pos = 0;
    const std::string pattern = "createErrorResponse";
    while ((pos = sourceCode.find(pattern, pos)) != std::string::npos) {
        count++;
        pos++;
    }
    // At least one definition + 3 call sites (one per catch block)
    EXPECT_GE(count, 4u) << "Expected at least 4 occurrences of 'createErrorResponse' in "
                         << "http_metadata_handler.cpp (1 definition + 3 call sites in catch blocks). "
                         << "Found: " << count << ". "
                         << "Each handler catch block must call createErrorResponse() to return "
                         << "a structured JSON error body instead of a raw exception message.";
}
