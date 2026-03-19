#include "../../../lib/http/http_metadata_handler.hpp"

#include <glaze/glaze.hpp>

#include <gtest/gtest.h>

#include <fstream>
#include <string>

// =============================================================================
// HttpMetadataHandler /cardinality endpoint tests
//
// Tests the synchronous formatCardinalityResponse() method (JSON structure)
// and source-level validation of handleCardinality() (parameter validation,
// error handling).  The HLL estimation itself is covered by
// query_optimization_test.cpp; these tests focus on the HTTP handler layer.
// =============================================================================

class HttpCardinalityHandlerTest : public ::testing::Test {
protected:
    HttpMetadataHandler handler{nullptr};
};

// ---------------------------------------------------------------------------
// formatCardinalityResponse — measurement-level cardinality
// ---------------------------------------------------------------------------

TEST_F(HttpCardinalityHandlerTest, FormatResponseMeasurementLevel) {
    std::unordered_map<std::string, double> tagCards;
    tagCards["host"] = 10.0;
    tagCards["region"] = 3.0;

    std::string json = handler.formatCardinalityResponse("cpu", 150.5, tagCards);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse cardinality response JSON";

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["measurement"].get<std::string>(), "cpu");
    EXPECT_DOUBLE_EQ(obj["estimated_series_count"].get<double>(), 150.5);

    auto& tagObj = obj["tag_cardinalities"].get<glz::generic::object_t>();
    EXPECT_EQ(tagObj.size(), 2u);
    EXPECT_DOUBLE_EQ(tagObj["host"].get<double>(), 10.0);
    EXPECT_DOUBLE_EQ(tagObj["region"].get<double>(), 3.0);
}

TEST_F(HttpCardinalityHandlerTest, FormatResponseTagSpecific) {
    // When querying a specific tag_key:tag_value combination, the handler
    // puts the compound key "tag_key:tag_value" into tag_cardinalities.
    std::unordered_map<std::string, double> tagCards;
    tagCards["region:us-west"] = 42.0;

    std::string json = handler.formatCardinalityResponse("temperature", 42.0, tagCards);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse tag-specific cardinality response JSON";

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["measurement"].get<std::string>(), "temperature");
    EXPECT_DOUBLE_EQ(obj["estimated_series_count"].get<double>(), 42.0);

    auto& tagObj = obj["tag_cardinalities"].get<glz::generic::object_t>();
    EXPECT_EQ(tagObj.size(), 1u);
    EXPECT_DOUBLE_EQ(tagObj["region:us-west"].get<double>(), 42.0);
}

TEST_F(HttpCardinalityHandlerTest, FormatResponseZeroCardinality) {
    std::unordered_map<std::string, double> tagCards;
    std::string json = handler.formatCardinalityResponse("nonexistent", 0.0, tagCards);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse zero cardinality response JSON";

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["measurement"].get<std::string>(), "nonexistent");
    EXPECT_DOUBLE_EQ(obj["estimated_series_count"].get<double>(), 0.0);

    auto& tagObj = obj["tag_cardinalities"].get<glz::generic::object_t>();
    EXPECT_TRUE(tagObj.empty());
}

TEST_F(HttpCardinalityHandlerTest, FormatResponseEmptyTagCardinalities) {
    // Measurement-level query with no tag keys returns empty tag_cardinalities
    std::unordered_map<std::string, double> tagCards;
    std::string json = handler.formatCardinalityResponse("disk", 500.0, tagCards);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["measurement"].get<std::string>(), "disk");
    EXPECT_DOUBLE_EQ(obj["estimated_series_count"].get<double>(), 500.0);

    auto& tagObj = obj["tag_cardinalities"].get<glz::generic::object_t>();
    EXPECT_TRUE(tagObj.empty());
}

TEST_F(HttpCardinalityHandlerTest, FormatResponseLargeCardinality) {
    // Verify large cardinality values serialize without precision loss
    std::unordered_map<std::string, double> tagCards;
    tagCards["host"] = 1000000.0;

    std::string json = handler.formatCardinalityResponse("metrics", 5000000.0, tagCards);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_DOUBLE_EQ(obj["estimated_series_count"].get<double>(), 5000000.0);

    auto& tagObj = obj["tag_cardinalities"].get<glz::generic::object_t>();
    EXPECT_DOUBLE_EQ(tagObj["host"].get<double>(), 1000000.0);
}

TEST_F(HttpCardinalityHandlerTest, FormatResponseManyTagKeys) {
    // Verify response handles many tag keys correctly
    std::unordered_map<std::string, double> tagCards;
    for (int i = 0; i < 20; ++i) {
        tagCards["tag_" + std::to_string(i)] = static_cast<double>(i * 10);
    }

    std::string json = handler.formatCardinalityResponse("system", 1000.0, tagCards);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    auto& tagObj = obj["tag_cardinalities"].get<glz::generic::object_t>();
    EXPECT_EQ(tagObj.size(), 20u);

    // Spot-check a few values
    EXPECT_DOUBLE_EQ(tagObj["tag_0"].get<double>(), 0.0);
    EXPECT_DOUBLE_EQ(tagObj["tag_5"].get<double>(), 50.0);
    EXPECT_DOUBLE_EQ(tagObj["tag_19"].get<double>(), 190.0);
}

// ---------------------------------------------------------------------------
// Error response format (reuses createErrorResponse, but verify structure
// matches what handleCardinality would return for error cases)
// ---------------------------------------------------------------------------

TEST_F(HttpCardinalityHandlerTest, MissingMeasurementErrorFormat) {
    // handleCardinality returns this error when measurement param is missing
    std::string json = handler.createErrorResponse("MISSING_PARAMETER",
                                                    "measurement parameter is required");

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "error");

    auto& errorObj = obj["error"].get<glz::generic::object_t>();
    EXPECT_EQ(errorObj["code"].get<std::string>(), "MISSING_PARAMETER");
    EXPECT_EQ(errorObj["message"].get<std::string>(), "measurement parameter is required");
}

TEST_F(HttpCardinalityHandlerTest, TagKeyWithoutTagValueErrorFormat) {
    // handleCardinality returns this error when tag_key provided without tag_value
    std::string json = handler.createErrorResponse("INVALID_PARAMETER",
                                                    "Both tag_key and tag_value must be provided together");

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "error");

    auto& errorObj = obj["error"].get<glz::generic::object_t>();
    EXPECT_EQ(errorObj["code"].get<std::string>(), "INVALID_PARAMETER");
    EXPECT_EQ(errorObj["message"].get<std::string>(),
              "Both tag_key and tag_value must be provided together");
}

TEST_F(HttpCardinalityHandlerTest, InvalidMeasurementParamErrorFormat) {
    // handleCardinality returns this error for measurement names with control chars
    std::string json = handler.createErrorResponse("INVALID_PARAMETER",
                                                    "Measurement name must not contain control characters");

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "error");

    auto& errorObj = obj["error"].get<glz::generic::object_t>();
    EXPECT_EQ(errorObj["code"].get<std::string>(), "INVALID_PARAMETER");
}

// ---------------------------------------------------------------------------
// validateQueryParam coverage for cardinality-specific parameters
// ---------------------------------------------------------------------------

TEST_F(HttpCardinalityHandlerTest, ValidateTagKeyAccepted) {
    auto err = HttpMetadataHandler::validateQueryParam("host", "Tag key");
    EXPECT_TRUE(err.empty()) << "Tag key 'host' should be valid: " << err;
}

TEST_F(HttpCardinalityHandlerTest, ValidateTagValueAccepted) {
    auto err = HttpMetadataHandler::validateQueryParam("server-01", "Tag value");
    EXPECT_TRUE(err.empty()) << "Tag value 'server-01' should be valid: " << err;
}

TEST_F(HttpCardinalityHandlerTest, ValidateTagKeyNullByteRejected) {
    std::string name = std::string("host") + '\0' + "evil";
    auto err = HttpMetadataHandler::validateQueryParam(name, "Tag key");
    EXPECT_FALSE(err.empty()) << "Tag key with null byte must be rejected";
}

TEST_F(HttpCardinalityHandlerTest, ValidateTagValueControlCharRejected) {
    std::string name = "server\n01";
    auto err = HttpMetadataHandler::validateQueryParam(name, "Tag value");
    EXPECT_FALSE(err.empty()) << "Tag value with newline must be rejected";
}

TEST_F(HttpCardinalityHandlerTest, ValidateEmptyTagKeyRejected) {
    auto err = HttpMetadataHandler::validateQueryParam("", "Tag key");
    EXPECT_FALSE(err.empty()) << "Empty tag key must be rejected";
}

// ---------------------------------------------------------------------------
// Source inspection: handleCardinality must call validateQueryParam
// ---------------------------------------------------------------------------

class CardinalityHandlerSourceTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
        std::ifstream file(HTTP_METADATA_HANDLER_SOURCE_PATH);
        ASSERT_TRUE(file.is_open()) << "Could not open http_metadata_handler.cpp at: "
                                    << HTTP_METADATA_HANDLER_SOURCE_PATH;
        sourceCode.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
        ASSERT_FALSE(sourceCode.empty());
    }

    // Extract the body of a named member function using brace matching
    std::string extractFunctionBody(const std::string& qualifiedName) {
        auto sigPos = sourceCode.rfind(qualifiedName);
        if (sigPos == std::string::npos) return "";

        auto closeParen = sourceCode.find(')', sigPos);
        if (closeParen == std::string::npos) return "";
        auto openBrace = sourceCode.find('{', closeParen);
        if (openBrace == std::string::npos) return "";

        int depth = 1;
        size_t pos = openBrace + 1;
        while (pos < sourceCode.size() && depth > 0) {
            if (sourceCode[pos] == '{') depth++;
            else if (sourceCode[pos] == '}') depth--;
            pos++;
        }
        return sourceCode.substr(openBrace, pos - openBrace);
    }
};

TEST_F(CardinalityHandlerSourceTest, HandleCardinalityCallsValidateQueryParam) {
    std::string body = extractFunctionBody("HttpMetadataHandler::handleCardinality");
    ASSERT_FALSE(body.empty()) << "Could not find HttpMetadataHandler::handleCardinality in source";

    EXPECT_NE(body.find("validateQueryParam"), std::string::npos)
        << "handleCardinality must call validateQueryParam() to reject malformed "
        << "measurement names and tag key/value parameters before engine calls.";
}

TEST_F(CardinalityHandlerSourceTest, HandleCardinalityChecksMissingMeasurement) {
    std::string body = extractFunctionBody("HttpMetadataHandler::handleCardinality");
    ASSERT_FALSE(body.empty());

    // Must check for empty measurement and return MISSING_PARAMETER error
    EXPECT_NE(body.find("MISSING_PARAMETER"), std::string::npos)
        << "handleCardinality must return MISSING_PARAMETER when measurement is empty";
}

TEST_F(CardinalityHandlerSourceTest, HandleCardinalityValidatesTagKeyTagValuePairing) {
    std::string body = extractFunctionBody("HttpMetadataHandler::handleCardinality");
    ASSERT_FALSE(body.empty());

    // Must validate that tag_key and tag_value are both present or both absent
    EXPECT_NE(body.find("tag_key"), std::string::npos)
        << "handleCardinality must read tag_key parameter";
    EXPECT_NE(body.find("tag_value"), std::string::npos)
        << "handleCardinality must read tag_value parameter";

    // Must contain the paired-validation check
    EXPECT_NE(body.find("Both tag_key and tag_value must be provided together"), std::string::npos)
        << "handleCardinality must enforce that tag_key and tag_value are provided together";
}

TEST_F(CardinalityHandlerSourceTest, HandleCardinalityCallsFormatCardinalityResponse) {
    std::string body = extractFunctionBody("HttpMetadataHandler::handleCardinality");
    ASSERT_FALSE(body.empty());

    EXPECT_NE(body.find("formatCardinalityResponse"), std::string::npos)
        << "handleCardinality must use formatCardinalityResponse() for consistent JSON output";
}

TEST_F(CardinalityHandlerSourceTest, HandleCardinalityScatterGatherAcrossShards) {
    std::string body = extractFunctionBody("HttpMetadataHandler::handleCardinality");
    ASSERT_FALSE(body.empty());

    // Must scatter-gather across shards (invoke_on or when_all)
    EXPECT_NE(body.find("invoke_on"), std::string::npos)
        << "handleCardinality must scatter-gather across shards via invoke_on";
    EXPECT_NE(body.find("when_all_succeed"), std::string::npos)
        << "handleCardinality must await all shard results with when_all_succeed";
}
