#include "../../../lib/http/http_metadata_handler.hpp"

#include <gtest/gtest.h>

#include <fstream>
#include <string>

// =============================================================================
// HttpMetadataHandler query-parameter validation tests
//
// The metadata endpoints (/tags, /fields) accept measurement names and tag
// names as URL query parameters.  If those parameters contain null bytes or
// other control characters they could corrupt index key lookups.
//
// Fix (Task #59): HttpMetadataHandler::validateQueryParam() must reject any
// name that contains a null byte (\x00) or ASCII control character (< 0x20).
// The handler functions handleTags() and handleFields() must call this
// validator and return HTTP 400 before performing any engine call.
// =============================================================================

class HttpMetadataHandlerValidationTest : public ::testing::Test {
protected:
    // Handler constructed with null engine (OK for pure validation tests
    // that do not exercise the async engine path).
    HttpMetadataHandler handler{nullptr};
};

// ---------------------------------------------------------------------------
// validateQueryParam — null byte rejection
// ---------------------------------------------------------------------------

TEST_F(HttpMetadataHandlerValidationTest, NullByteInMeasurementNameRejected) {
    std::string name = std::string("temperature") + '\0' + "../secret";
    auto err = HttpMetadataHandler::validateQueryParam(name, "measurement");
    EXPECT_FALSE(err.empty()) << "validateQueryParam must reject names containing null bytes";
    EXPECT_NE(err.find("null"), std::string::npos) << "Error message should mention 'null'";
}

TEST_F(HttpMetadataHandlerValidationTest, NullByteAtStartRejected) {
    std::string name = std::string("\0cpu", 4);
    auto err = HttpMetadataHandler::validateQueryParam(name, "measurement");
    EXPECT_FALSE(err.empty()) << "validateQueryParam must reject names beginning with a null byte";
}

TEST_F(HttpMetadataHandlerValidationTest, NullByteAtEndRejected) {
    std::string name = std::string("cpu\0", 4);
    auto err = HttpMetadataHandler::validateQueryParam(name, "measurement");
    EXPECT_FALSE(err.empty()) << "validateQueryParam must reject names ending with a null byte";
}

// ---------------------------------------------------------------------------
// validateQueryParam — control character rejection
// ---------------------------------------------------------------------------

TEST_F(HttpMetadataHandlerValidationTest, ControlCharNewlineRejected) {
    std::string name = "cpu\nusage";
    auto err = HttpMetadataHandler::validateQueryParam(name, "measurement");
    EXPECT_FALSE(err.empty()) << "validateQueryParam must reject names containing newline (\\n)";
}

TEST_F(HttpMetadataHandlerValidationTest, ControlCharTabRejected) {
    std::string name = "cpu\tusage";
    auto err = HttpMetadataHandler::validateQueryParam(name, "measurement");
    EXPECT_FALSE(err.empty()) << "validateQueryParam must reject names containing tab (\\t)";
}

TEST_F(HttpMetadataHandlerValidationTest, ControlCharCarriageReturnRejected) {
    std::string name = "cpu\rusage";
    auto err = HttpMetadataHandler::validateQueryParam(name, "measurement");
    EXPECT_FALSE(err.empty()) << "validateQueryParam must reject names containing carriage return (\\r)";
}

// ---------------------------------------------------------------------------
// validateQueryParam — empty name rejection
// ---------------------------------------------------------------------------

TEST_F(HttpMetadataHandlerValidationTest, EmptyNameRejected) {
    auto err = HttpMetadataHandler::validateQueryParam("", "measurement");
    EXPECT_FALSE(err.empty()) << "validateQueryParam must reject empty names";
}

// ---------------------------------------------------------------------------
// validateQueryParam — valid names must pass
// ---------------------------------------------------------------------------

TEST_F(HttpMetadataHandlerValidationTest, ValidSimpleNameAccepted) {
    auto err = HttpMetadataHandler::validateQueryParam("temperature", "measurement");
    EXPECT_TRUE(err.empty()) << "validateQueryParam must accept plain ASCII names: " << err;
}

TEST_F(HttpMetadataHandlerValidationTest, ValidNameWithDotsAccepted) {
    auto err = HttpMetadataHandler::validateQueryParam("system.cpu.usage", "measurement");
    EXPECT_TRUE(err.empty()) << "validateQueryParam must accept names with dots: " << err;
}

TEST_F(HttpMetadataHandlerValidationTest, ValidNameWithHyphensAccepted) {
    auto err = HttpMetadataHandler::validateQueryParam("cpu-usage-total", "measurement");
    EXPECT_TRUE(err.empty()) << "validateQueryParam must accept names with hyphens: " << err;
}

TEST_F(HttpMetadataHandlerValidationTest, ValidNameWithUnderscoresAccepted) {
    auto err = HttpMetadataHandler::validateQueryParam("cpu_usage", "measurement");
    EXPECT_TRUE(err.empty()) << "validateQueryParam must accept names with underscores: " << err;
}

TEST_F(HttpMetadataHandlerValidationTest, ValidNameWithDigitsAccepted) {
    auto err = HttpMetadataHandler::validateQueryParam("server01", "measurement");
    EXPECT_TRUE(err.empty()) << "validateQueryParam must accept names with digits: " << err;
}

TEST_F(HttpMetadataHandlerValidationTest, ValidTagNameAccepted) {
    auto err = HttpMetadataHandler::validateQueryParam("location", "tag");
    EXPECT_TRUE(err.empty()) << "validateQueryParam must accept simple tag name: " << err;
}

// ---------------------------------------------------------------------------
// Source inspection: handleTags and handleFields must call validateQueryParam
// ---------------------------------------------------------------------------

class MetadataHandlerNullByteSourceTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
        std::ifstream file(HTTP_METADATA_HANDLER_SOURCE_PATH);
        ASSERT_TRUE(file.is_open()) << "Could not open http_metadata_handler.cpp at: "
                                    << HTTP_METADATA_HANDLER_SOURCE_PATH;
        sourceCode.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
        ASSERT_FALSE(sourceCode.empty());
    }
};

TEST_F(MetadataHandlerNullByteSourceTest, HandleTagsCallsValidateQueryParam) {
    // Find handleTags function body
    auto sigPos = sourceCode.rfind("HttpMetadataHandler::handleTags");
    ASSERT_NE(sigPos, std::string::npos) << "Could not find HttpMetadataHandler::handleTags in source";

    // Find the opening brace of the function body
    auto closeParen = sourceCode.find(')', sigPos);
    ASSERT_NE(closeParen, std::string::npos);
    auto openBrace = sourceCode.find('{', closeParen);
    ASSERT_NE(openBrace, std::string::npos);

    // Brace-match to extract the full function body
    int depth = 1;
    size_t pos = openBrace + 1;
    while (pos < sourceCode.size() && depth > 0) {
        if (sourceCode[pos] == '{')
            depth++;
        else if (sourceCode[pos] == '}')
            depth--;
        pos++;
    }
    std::string body = sourceCode.substr(openBrace, pos - openBrace);

    EXPECT_NE(body.find("validateQueryParam"), std::string::npos)
        << "handleTags must call validateQueryParam() to reject null bytes in "
        << "the 'measurement' (and optionally 'tag') query parameters before "
        << "passing them to the index engine.";
}

TEST_F(MetadataHandlerNullByteSourceTest, HandleFieldsCallsValidateQueryParam) {
    // Find handleFields function body
    auto sigPos = sourceCode.rfind("HttpMetadataHandler::handleFields");
    ASSERT_NE(sigPos, std::string::npos) << "Could not find HttpMetadataHandler::handleFields in source";

    auto closeParen = sourceCode.find(')', sigPos);
    ASSERT_NE(closeParen, std::string::npos);
    auto openBrace = sourceCode.find('{', closeParen);
    ASSERT_NE(openBrace, std::string::npos);

    int depth = 1;
    size_t pos = openBrace + 1;
    while (pos < sourceCode.size() && depth > 0) {
        if (sourceCode[pos] == '{')
            depth++;
        else if (sourceCode[pos] == '}')
            depth--;
        pos++;
    }
    std::string body = sourceCode.substr(openBrace, pos - openBrace);

    EXPECT_NE(body.find("validateQueryParam"), std::string::npos)
        << "handleFields must call validateQueryParam() to reject null bytes in "
        << "the 'measurement' query parameter before passing it to the index "
        << "engine.";
}
