#include <glaze/glaze.hpp>

#include <gtest/gtest.h>

// Define GlazeDeleteRequest before including the handler header.
// The header forward-declares this struct, and parseDeleteRequest takes it by const ref.
// This definition must match the one in http_delete_handler.cpp exactly (ODR-compliant).
struct GlazeDeleteRequest {
    // For series key format
    std::optional<std::string> series;

    // For structured format
    std::optional<std::string> measurement;
    std::optional<std::map<std::string, std::string>> tags;
    std::optional<std::string> field;
    std::optional<std::vector<std::string>> fields;

    // Time range (optional - defaults to all time)
    std::optional<uint64_t> startTime;
    std::optional<uint64_t> endTime;
};

template <>
struct glz::meta<GlazeDeleteRequest> {
    using T = GlazeDeleteRequest;
    static constexpr auto value =
        object("series", &T::series, "measurement", &T::measurement, "tags", &T::tags, "field", &T::field, "fields",
               &T::fields, "startTime", &T::startTime, "endTime", &T::endTime);
};

#include "../../../lib/http/http_delete_handler.hpp"

// =============================================================================
// HttpDeleteHandler Tests
// Tests for synchronous parsing and response formatting methods
// =============================================================================

class HttpDeleteHandlerTest : public ::testing::Test {
protected:
    HttpDeleteHandler handler{nullptr};

    // Helper: parse a JSON string into a GlazeDeleteRequest
    GlazeDeleteRequest parseJson(const std::string& json) {
        GlazeDeleteRequest req;
        auto ec = glz::read_json(req, json);
        EXPECT_FALSE(ec) << "Failed to parse JSON: " << glz::format_error(ec);
        return req;
    }
};

// ---------------------------------------------------------------------------
// parseDeleteRequest tests
// ---------------------------------------------------------------------------

TEST_F(HttpDeleteHandlerTest, ParseSeriesKeyFormat) {
    auto glazeReq = parseJson(R"({"series": "cpu,host=server01 value"})");

    auto req = handler.parseDeleteRequest(glazeReq);

    EXPECT_FALSE(req.isStructured);
    EXPECT_FALSE(req.isPattern);
    EXPECT_EQ(req.seriesKey, "cpu,host=server01 value");
    // Default time range
    EXPECT_EQ(req.startTime, 0u);
    EXPECT_EQ(req.endTime, UINT64_MAX);
}

TEST_F(HttpDeleteHandlerTest, ParseStructuredFormat) {
    auto glazeReq = parseJson(R"({
        "measurement": "cpu",
        "tags": {"host": "server01", "region": "us-west"},
        "field": "value"
    })");

    auto req = handler.parseDeleteRequest(glazeReq);

    EXPECT_TRUE(req.isStructured);
    EXPECT_FALSE(req.isPattern);
    EXPECT_EQ(req.measurement, "cpu");
    EXPECT_EQ(req.tags.size(), 2u);
    EXPECT_EQ(req.tags.at("host"), "server01");
    EXPECT_EQ(req.tags.at("region"), "us-west");
    EXPECT_EQ(req.field, "value");
    // When a single "field" is provided, it also populates the fields vector
    EXPECT_EQ(req.fields.size(), 1u);
    EXPECT_EQ(req.fields[0], "value");
}

TEST_F(HttpDeleteHandlerTest, ParsePatternFormat) {
    auto glazeReq = parseJson(R"({
        "measurement": "cpu",
        "tags": {"host": "server01"},
        "fields": ["value", "idle"]
    })");

    auto req = handler.parseDeleteRequest(glazeReq);

    EXPECT_TRUE(req.isStructured);
    EXPECT_TRUE(req.isPattern);
    EXPECT_EQ(req.measurement, "cpu");
    EXPECT_EQ(req.fields.size(), 2u);
    EXPECT_EQ(req.fields[0], "value");
    EXPECT_EQ(req.fields[1], "idle");
    // First field set for backward compatibility
    EXPECT_EQ(req.field, "value");
}

TEST_F(HttpDeleteHandlerTest, ParsePatternFormatNoFields) {
    auto glazeReq = parseJson(R"({"measurement": "cpu"})");

    auto req = handler.parseDeleteRequest(glazeReq);

    EXPECT_TRUE(req.isStructured);
    EXPECT_TRUE(req.isPattern);
    EXPECT_EQ(req.measurement, "cpu");
    // Empty fields means match all fields
    EXPECT_TRUE(req.fields.empty());
    EXPECT_TRUE(req.field.empty());
}

TEST_F(HttpDeleteHandlerTest, ParseTimeRangeDefaults) {
    auto glazeReq = parseJson(R"({"series": "cpu value"})");

    auto req = handler.parseDeleteRequest(glazeReq);

    EXPECT_EQ(req.startTime, 0u);
    EXPECT_EQ(req.endTime, UINT64_MAX);
}

TEST_F(HttpDeleteHandlerTest, ParseTimeRangeExplicit) {
    auto glazeReq = parseJson(R"({
        "series": "cpu value",
        "startTime": 1000000,
        "endTime": 2000000
    })");

    auto req = handler.parseDeleteRequest(glazeReq);

    EXPECT_EQ(req.startTime, 1000000u);
    EXPECT_EQ(req.endTime, 2000000u);
}

TEST_F(HttpDeleteHandlerTest, ParseTimeRangeValidation) {
    auto glazeReq = parseJson(R"({
        "series": "cpu value",
        "startTime": 5000000,
        "endTime": 1000000
    })");

    EXPECT_THROW(handler.parseDeleteRequest(glazeReq), std::runtime_error);
}

TEST_F(HttpDeleteHandlerTest, ParseMissingRequiredFields) {
    // Neither "series" nor "measurement" provided
    auto glazeReq = parseJson(R"({"startTime": 1000})");

    EXPECT_THROW(handler.parseDeleteRequest(glazeReq), std::runtime_error);
}

// ---------------------------------------------------------------------------
// createErrorResponse tests
// ---------------------------------------------------------------------------

TEST_F(HttpDeleteHandlerTest, CreateErrorResponse) {
    std::string json = handler.createErrorResponse("test error");

    // Parse the JSON output
    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse error response JSON";

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "error");
    EXPECT_EQ(obj["error"].get<std::string>(), "test error");
}

TEST_F(HttpDeleteHandlerTest, CreateErrorResponseSpecialCharacters) {
    std::string json = handler.createErrorResponse("field 'name' contains \"invalid\" chars: <>&");

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse error response with special chars";

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "error");
    EXPECT_NE(obj["error"].get<std::string>().find("invalid"), std::string::npos);
}

// ---------------------------------------------------------------------------
// createSuccessResponse tests
// ---------------------------------------------------------------------------

TEST_F(HttpDeleteHandlerTest, CreateSuccessResponse) {
    std::string json = handler.createSuccessResponse(5, 10);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse success response JSON";

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "success");
    EXPECT_EQ(static_cast<int>(obj["deleted"].get<double>()), 5);
    EXPECT_EQ(static_cast<int>(obj["total"].get<double>()), 10);
}

TEST_F(HttpDeleteHandlerTest, CreateSuccessResponseZeroCounts) {
    std::string json = handler.createSuccessResponse(0, 0);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "success");
    EXPECT_EQ(static_cast<int>(obj["deleted"].get<double>()), 0);
    EXPECT_EQ(static_cast<int>(obj["total"].get<double>()), 0);
}

// ---------------------------------------------------------------------------
// JSON round-trip: parse JSON -> parseDeleteRequest -> verify
// ---------------------------------------------------------------------------

TEST_F(HttpDeleteHandlerTest, RoundTripStructuredWithAllFields) {
    std::string input = R"({
        "measurement": "temperature",
        "tags": {"location": "us-west", "host": "server-01"},
        "field": "humidity",
        "startTime": 1465839830100400200,
        "endTime": 1465839930100400200
    })";

    auto glazeReq = parseJson(input);
    auto req = handler.parseDeleteRequest(glazeReq);

    EXPECT_TRUE(req.isStructured);
    EXPECT_FALSE(req.isPattern);
    EXPECT_EQ(req.measurement, "temperature");
    EXPECT_EQ(req.tags.at("location"), "us-west");
    EXPECT_EQ(req.tags.at("host"), "server-01");
    EXPECT_EQ(req.field, "humidity");
    EXPECT_EQ(req.startTime, 1465839830100400200ULL);
    EXPECT_EQ(req.endTime, 1465839930100400200ULL);
}

TEST_F(HttpDeleteHandlerTest, ParseStructuredNoTags) {
    auto glazeReq = parseJson(R"({
        "measurement": "cpu",
        "field": "usage"
    })");

    auto req = handler.parseDeleteRequest(glazeReq);

    EXPECT_TRUE(req.isStructured);
    EXPECT_FALSE(req.isPattern);
    EXPECT_EQ(req.measurement, "cpu");
    EXPECT_TRUE(req.tags.empty());
    EXPECT_EQ(req.field, "usage");
}
