#include <gtest/gtest.h>
#include <glaze/glaze.hpp>
#include "../../../lib/http/http_metadata_handler.hpp"

// =============================================================================
// HttpMetadataHandler Tests
// Tests for synchronous response formatting methods
// =============================================================================

class HttpMetadataHandlerTest : public ::testing::Test {
protected:
    HttpMetadataHandler handler{nullptr};
};

// ---------------------------------------------------------------------------
// createErrorResponse tests
// ---------------------------------------------------------------------------

TEST_F(HttpMetadataHandlerTest, CreateErrorResponse) {
    std::string json = handler.createErrorResponse("INVALID", "bad request");

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse error response JSON";

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "error");

    auto& errorObj = obj["error"].get<glz::generic::object_t>();
    EXPECT_EQ(errorObj["code"].get<std::string>(), "INVALID");
    EXPECT_EQ(errorObj["message"].get<std::string>(), "bad request");
}

TEST_F(HttpMetadataHandlerTest, CreateErrorResponseInternalError) {
    std::string json = handler.createErrorResponse("INTERNAL_ERROR", "something went wrong");

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "error");

    auto& errorObj = obj["error"].get<glz::generic::object_t>();
    EXPECT_EQ(errorObj["code"].get<std::string>(), "INTERNAL_ERROR");
    EXPECT_EQ(errorObj["message"].get<std::string>(), "something went wrong");
}

// ---------------------------------------------------------------------------
// formatMeasurementsResponse tests
// ---------------------------------------------------------------------------

TEST_F(HttpMetadataHandlerTest, FormatMeasurementsResponse) {
    std::vector<std::string> measurements = {"cpu", "memory", "disk"};
    std::string json = handler.formatMeasurementsResponse(measurements, 3);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse measurements response JSON";

    auto& obj = parsed.get<glz::generic::object_t>();

    auto& mArr = obj["measurements"].get<glz::generic::array_t>();
    EXPECT_EQ(mArr.size(), 3u);
    EXPECT_EQ(mArr[0].get<std::string>(), "cpu");
    EXPECT_EQ(mArr[1].get<std::string>(), "memory");
    EXPECT_EQ(mArr[2].get<std::string>(), "disk");

    EXPECT_EQ(static_cast<size_t>(obj["total"].get<double>()), 3u);
}

TEST_F(HttpMetadataHandlerTest, FormatMeasurementsResponseEmpty) {
    std::vector<std::string> measurements;
    std::string json = handler.formatMeasurementsResponse(measurements, 0);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();

    auto& mArr = obj["measurements"].get<glz::generic::array_t>();
    EXPECT_TRUE(mArr.empty());
    EXPECT_EQ(static_cast<size_t>(obj["total"].get<double>()), 0u);
}

TEST_F(HttpMetadataHandlerTest, FormatMeasurementsResponsePaginated) {
    // Simulate a paginated response where total > returned items
    std::vector<std::string> measurements = {"cpu", "memory"};
    std::string json = handler.formatMeasurementsResponse(measurements, 5);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();

    auto& mArr = obj["measurements"].get<glz::generic::array_t>();
    EXPECT_EQ(mArr.size(), 2u);
    EXPECT_EQ(static_cast<size_t>(obj["total"].get<double>()), 5u);
}

// ---------------------------------------------------------------------------
// formatTagsResponse tests
// ---------------------------------------------------------------------------

TEST_F(HttpMetadataHandlerTest, FormatTagsResponseAllTags) {
    std::unordered_map<std::string, std::vector<std::string>> tags;
    tags["host"] = {"server01", "server02"};
    tags["region"] = {"us-east", "eu-west"};

    std::string json = handler.formatTagsResponse("cpu", tags, "");

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse tags response JSON";

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["measurement"].get<std::string>(), "cpu");

    auto& tagsObj = obj["tags"].get<glz::generic::object_t>();
    EXPECT_EQ(tagsObj.size(), 2u);

    // Verify host tag values
    auto& hostValues = tagsObj["host"].get<glz::generic::array_t>();
    EXPECT_EQ(hostValues.size(), 2u);

    // Verify region tag values
    auto& regionValues = tagsObj["region"].get<glz::generic::array_t>();
    EXPECT_EQ(regionValues.size(), 2u);
}

TEST_F(HttpMetadataHandlerTest, FormatTagsResponseSpecificTag) {
    std::unordered_map<std::string, std::vector<std::string>> tags;
    tags["host"] = {"server01", "server02"};

    std::string json = handler.formatTagsResponse("cpu", tags, "host");

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse specific tag response JSON";

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["measurement"].get<std::string>(), "cpu");
    EXPECT_EQ(obj["tag"].get<std::string>(), "host");

    auto& values = obj["values"].get<glz::generic::array_t>();
    EXPECT_EQ(values.size(), 2u);
    EXPECT_EQ(values[0].get<std::string>(), "server01");
    EXPECT_EQ(values[1].get<std::string>(), "server02");
}

TEST_F(HttpMetadataHandlerTest, FormatTagsResponseSpecificTagNotFound) {
    // When querying a specific tag that doesn't exist in the map
    std::unordered_map<std::string, std::vector<std::string>> tags;
    // Empty map -- tag not found

    std::string json = handler.formatTagsResponse("cpu", tags, "nonexistent");

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["measurement"].get<std::string>(), "cpu");
    EXPECT_EQ(obj["tag"].get<std::string>(), "nonexistent");

    auto& values = obj["values"].get<glz::generic::array_t>();
    EXPECT_TRUE(values.empty());
}

TEST_F(HttpMetadataHandlerTest, FormatTagsResponseEmptyTags) {
    std::unordered_map<std::string, std::vector<std::string>> tags;

    std::string json = handler.formatTagsResponse("cpu", tags, "");

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["measurement"].get<std::string>(), "cpu");

    auto& tagsObj = obj["tags"].get<glz::generic::object_t>();
    EXPECT_TRUE(tagsObj.empty());
}

// ---------------------------------------------------------------------------
// formatFieldsResponse tests
// ---------------------------------------------------------------------------

TEST_F(HttpMetadataHandlerTest, FormatFieldsResponseBasic) {
    std::unordered_map<std::string, std::string> fields;
    fields["value"] = "float";
    fields["count"] = "integer";

    std::string json = handler.formatFieldsResponse("cpu", fields);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse fields response JSON";

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["measurement"].get<std::string>(), "cpu");

    auto& fieldsObj = obj["fields"].get<glz::generic::object_t>();
    EXPECT_EQ(fieldsObj.size(), 2u);

    // Check that each field has name and type
    auto& valueField = fieldsObj["value"].get<glz::generic::object_t>();
    EXPECT_EQ(valueField["name"].get<std::string>(), "value");
    EXPECT_EQ(valueField["type"].get<std::string>(), "float");

    auto& countField = fieldsObj["count"].get<glz::generic::object_t>();
    EXPECT_EQ(countField["name"].get<std::string>(), "count");
    EXPECT_EQ(countField["type"].get<std::string>(), "integer");
}

TEST_F(HttpMetadataHandlerTest, FormatFieldsResponseWithFilters) {
    std::unordered_map<std::string, std::string> fields;
    fields["value"] = "float";

    std::unordered_map<std::string, std::string> tagFilters;
    tagFilters["host"] = "server01";

    std::string json = handler.formatFieldsResponse("cpu", fields, tagFilters);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse filtered fields response JSON";

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["measurement"].get<std::string>(), "cpu");

    // Verify filtered_by is present
    auto& filteredBy = obj["filtered_by"].get<glz::generic::object_t>();
    EXPECT_EQ(filteredBy["host"].get<std::string>(), "server01");
}

TEST_F(HttpMetadataHandlerTest, FormatFieldsResponseNoFilters) {
    std::unordered_map<std::string, std::string> fields;
    fields["temperature"] = "float";

    std::string json = handler.formatFieldsResponse("weather", fields);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["measurement"].get<std::string>(), "weather");

    // filtered_by should be absent or null when no filters
    EXPECT_TRUE(obj.find("filtered_by") == obj.end() ||
                obj["filtered_by"].is_null());
}

TEST_F(HttpMetadataHandlerTest, FormatFieldsResponseEmpty) {
    std::unordered_map<std::string, std::string> fields;

    std::string json = handler.formatFieldsResponse("cpu", fields);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["measurement"].get<std::string>(), "cpu");

    auto& fieldsObj = obj["fields"].get<glz::generic::object_t>();
    EXPECT_TRUE(fieldsObj.empty());
}

// ---------------------------------------------------------------------------
// parsePaginationParam tests (validates safe std::stoul wrapping)
// ---------------------------------------------------------------------------

TEST_F(HttpMetadataHandlerTest, ParsePaginationParamValidNumber) {
    // Valid numeric string should parse correctly
    size_t result = HttpMetadataHandler::parsePaginationParam("42", "limit", 100);
    EXPECT_EQ(result, 42u);
}

TEST_F(HttpMetadataHandlerTest, ParsePaginationParamZero) {
    // Zero is a valid value
    size_t result = HttpMetadataHandler::parsePaginationParam("0", "offset", 0);
    EXPECT_EQ(result, 0u);
}

TEST_F(HttpMetadataHandlerTest, ParsePaginationParamDefault) {
    // Empty string should return the default value
    size_t result = HttpMetadataHandler::parsePaginationParam("", "limit", 100);
    EXPECT_EQ(result, 100u);
}

TEST_F(HttpMetadataHandlerTest, ParsePaginationParamInvalidThrows) {
    // Non-numeric string must throw HttpBadRequestException, not crash
    EXPECT_THROW(
        HttpMetadataHandler::parsePaginationParam("abc", "limit", 100),
        HttpMetadataHandler::BadRequestException
    );
}

TEST_F(HttpMetadataHandlerTest, ParsePaginationParamNegativeThrows) {
    // Negative numbers must throw (stoul rejects them)
    EXPECT_THROW(
        HttpMetadataHandler::parsePaginationParam("-1", "offset", 0),
        HttpMetadataHandler::BadRequestException
    );
}

TEST_F(HttpMetadataHandlerTest, ParsePaginationParamFloatThrows) {
    // Floating-point strings must throw
    EXPECT_THROW(
        HttpMetadataHandler::parsePaginationParam("3.14", "limit", 100),
        HttpMetadataHandler::BadRequestException
    );
}

TEST_F(HttpMetadataHandlerTest, ParsePaginationParamAlphanumericThrows) {
    // Alphanumeric strings must throw
    EXPECT_THROW(
        HttpMetadataHandler::parsePaginationParam("10abc", "limit", 100),
        HttpMetadataHandler::BadRequestException
    );
}

TEST_F(HttpMetadataHandlerTest, ParsePaginationParamOverflowThrows) {
    // Values that overflow size_t must throw
    EXPECT_THROW(
        HttpMetadataHandler::parsePaginationParam("99999999999999999999999999999", "limit", 100),
        HttpMetadataHandler::BadRequestException
    );
}
