#include <gtest/gtest.h>
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/query/query_parser.hpp"
#include <glaze/glaze.hpp>

using namespace tsdb;

// Glaze-compatible structure for JSON parsing (from http_query_handler.cpp)
struct GlazeQueryRequest {
    std::string query;
    std::variant<uint64_t, std::string> startTime;
    std::variant<uint64_t, std::string> endTime;
    std::optional<std::variant<uint64_t, std::string>> aggregationInterval;
};

template <>
struct glz::meta<GlazeQueryRequest> {
    using T = GlazeQueryRequest;
    static constexpr auto value = object(
        "query", &T::query,
        "startTime", &T::startTime,
        "endTime", &T::endTime,
        "aggregationInterval", &T::aggregationInterval
    );
};

// Glaze structures for parsing HTTP responses (matching http_query_handler.cpp structures)
struct GlazeFieldData {
    std::vector<uint64_t> timestamps;
    std::variant<std::vector<double>, std::vector<bool>, std::vector<std::string>> values;
};

struct GlazeSeriesData {
    std::string measurement;
    std::vector<std::string> groupTags;  // Changed from tags map to groupTags array
    std::map<std::string, GlazeFieldData> fields;
};

struct GlazeStatistics {
    int64_t series_count;
    int64_t point_count;
    double execution_time_ms;
};

struct GlazeQueryResponse {
    std::string status;
    std::vector<GlazeSeriesData> series;
    GlazeStatistics statistics;
};

struct GlazeErrorResponse {
    std::string status;
    std::string message;
    std::string error;
};

// Glaze meta declarations
template <>
struct glz::meta<GlazeFieldData> {
    using T = GlazeFieldData;
    static constexpr auto value = object(
        "timestamps", &T::timestamps,
        "values", &T::values
    );
};

template <>
struct glz::meta<GlazeSeriesData> {
    using T = GlazeSeriesData;
    static constexpr auto value = object(
        "measurement", &T::measurement,
        "groupTags", &T::groupTags,
        "fields", &T::fields
    );
};

template <>
struct glz::meta<GlazeStatistics> {
    using T = GlazeStatistics;
    static constexpr auto value = object(
        "series_count", &T::series_count,
        "point_count", &T::point_count,
        "execution_time_ms", &T::execution_time_ms
    );
};

template <>
struct glz::meta<GlazeQueryResponse> {
    using T = GlazeQueryResponse;
    static constexpr auto value = object(
        "status", &T::status,
        "series", &T::series,
        "statistics", &T::statistics
    );
};

template <>
struct glz::meta<GlazeErrorResponse> {
    using T = GlazeErrorResponse;
    static constexpr auto value = object(
        "status", &T::status,
        "message", &T::message,
        "error", &T::error
    );
};

class HttpQueryHandlerTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
    
    // Helper to create JSON string using Glaze
    std::string createJsonRequest(const std::string& query, 
                                 const std::string& startTime,
                                 const std::string& endTime) {
        GlazeQueryRequest req;
        req.query = query;
        req.startTime = startTime;
        req.endTime = endTime;
        
        return glz::write_json(req).value_or("{}");
    }
};

// Test QueryResponse formatting
TEST_F(HttpQueryHandlerTest, FormatEmptyResponse) {
    HttpQueryHandler handler(nullptr);  // Using nullptr for mock
    
    QueryResponse response;
    response.success = true;
    response.statistics.seriesCount = 0;
    response.statistics.pointCount = 0;
    response.statistics.executionTimeMs = 5.5;
    
    std::string json = handler.formatQueryResponse(response);
    
    // Parse and verify using Glaze
    GlazeQueryResponse parsedResponse;
    auto error = glz::read_json(parsedResponse, json);
    
    ASSERT_FALSE(error) << "JSON parse error: " << glz::format_error(error);
    EXPECT_EQ(parsedResponse.status, "success");
    EXPECT_EQ(parsedResponse.series.size(), 0);
    EXPECT_EQ(parsedResponse.statistics.series_count, 0);
    EXPECT_EQ(parsedResponse.statistics.point_count, 0);
    EXPECT_DOUBLE_EQ(parsedResponse.statistics.execution_time_ms, 5.5);
}

TEST_F(HttpQueryHandlerTest, FormatResponseWithData) {
    HttpQueryHandler handler(nullptr);
    
    QueryResponse response;
    response.success = true;
    
    // Create a series with data
    SeriesResult series;
    series.measurement = "temperature";
    series.tags["location"] = "office";
    series.tags["sensor"] = "temp-01";
    
    std::vector<uint64_t> timestamps = {1000000000, 2000000000, 3000000000};
    std::vector<double> values = {20.5, 21.0, 21.5};
    series.fields["value"] = std::make_pair(timestamps, values);
    
    response.series.push_back(series);
    response.statistics.seriesCount = 1;
    response.statistics.pointCount = 3;
    response.statistics.executionTimeMs = 10.0;
    
    std::string json = handler.formatQueryResponse(response);
    
    // Parse and verify using Glaze
    GlazeQueryResponse parsedResponse;
    auto error = glz::read_json(parsedResponse, json);
    
    ASSERT_FALSE(error) << "JSON parse error: " << glz::format_error(error);
    EXPECT_EQ(parsedResponse.status, "success");
    EXPECT_EQ(parsedResponse.series.size(), 1);
    
    const auto& seriesObj = parsedResponse.series[0];
    EXPECT_EQ(seriesObj.measurement, "temperature");
    // The response format uses groupTags array instead of tags map, so check groupTags
    EXPECT_EQ(seriesObj.groupTags.size(), 2);
    EXPECT_TRUE(std::find(seriesObj.groupTags.begin(), seriesObj.groupTags.end(), "location=office") != seriesObj.groupTags.end());
    EXPECT_TRUE(std::find(seriesObj.groupTags.begin(), seriesObj.groupTags.end(), "sensor=temp-01") != seriesObj.groupTags.end());
    
    const auto& fieldData = seriesObj.fields.at("value");
    EXPECT_EQ(fieldData.timestamps.size(), 3);
    auto& doubleValues = std::get<std::vector<double>>(fieldData.values);
    EXPECT_EQ(doubleValues.size(), 3);
    EXPECT_EQ(fieldData.timestamps[0], 1000000000);
    EXPECT_DOUBLE_EQ(doubleValues[0], 20.5);
}

TEST_F(HttpQueryHandlerTest, FormatErrorResponse) {
    HttpQueryHandler handler(nullptr);
    
    std::string json = handler.createErrorResponse("INVALID_QUERY", "Missing measurement");
    
    // Parse and verify using Glaze
    GlazeErrorResponse parsedResponse;
    auto error = glz::read_json(parsedResponse, json);
    
    ASSERT_FALSE(error) << "JSON parse error: " << glz::format_error(error);
    EXPECT_EQ(parsedResponse.status, "error");
    EXPECT_EQ(parsedResponse.error, "Missing measurement");
    EXPECT_EQ(parsedResponse.message, "Missing measurement");
}

// Test request parsing
TEST_F(HttpQueryHandlerTest, ParseValidQueryRequest) {
    HttpQueryHandler handler(nullptr);
    
    std::string jsonStr = createJsonRequest(
        "avg:temperature(value){location:office}",
        "1640995200000000000",
        "1641081599000000000"
    );
    
    GlazeQueryRequest glazeReq;
    auto error = glz::read_json(glazeReq, jsonStr);
    ASSERT_FALSE(error);
    
    QueryRequest request = handler.parseQueryRequest(glazeReq);
    
    EXPECT_EQ(request.measurement, "temperature");
    EXPECT_EQ(request.fields.size(), 1);
    EXPECT_EQ(request.fields[0], "value");
    EXPECT_EQ(request.aggregation, AggregationMethod::AVG);
    EXPECT_EQ(request.scopes.size(), 1);
    EXPECT_EQ(request.scopes.at("location"), "office");
    EXPECT_EQ(request.startTime, 1640995200000000000ULL);
    EXPECT_EQ(request.endTime, 1641081599000000000ULL);
}

// Test shard determination
TEST_F(HttpQueryHandlerTest, DetermineTargetShards) {
    HttpQueryHandler handler(nullptr);
    
    QueryRequest request;
    request.measurement = "temperature";
    
    // With no filters, should target all shards
    auto shards = handler.determineTargetShards(request);
    EXPECT_GT(shards.size(), 0);
    
    // Verify all shards are included
    unsigned shardCount = seastar::smp::count;
    if (shardCount == 0) shardCount = 1; // Test environment default
    for (unsigned i = 0; i < shardCount; ++i) {
        EXPECT_TRUE(std::find(shards.begin(), shards.end(), i) != shards.end());
    }
}

// Test result merging
TEST_F(HttpQueryHandlerTest, MergeResults) {
    HttpQueryHandler handler(nullptr);
    
    // Create results from different shards
    std::vector<std::vector<SeriesResult>> shardResults(2);
    
    // Shard 0 results
    SeriesResult series1;
    series1.measurement = "temperature";
    series1.tags["location"] = "office";
    std::vector<uint64_t> ts1 = {1000, 2000};
    std::vector<double> vals1 = {20.0, 21.0};
    series1.fields["value"] = std::make_pair(ts1, vals1);
    shardResults[0].push_back(series1);
    
    // Shard 1 results  
    SeriesResult series2;
    series2.measurement = "temperature";
    series2.tags["location"] = "warehouse";
    std::vector<uint64_t> ts2 = {1000, 2000};
    std::vector<double> vals2 = {18.0, 19.0};
    series2.fields["value"] = std::make_pair(ts2, vals2);
    shardResults[1].push_back(series2);
    
    auto merged = handler.mergeResults(shardResults);

    EXPECT_EQ(merged.size(), 2);  // Should have both series
}

// =============================================================================
// Request Body Size Limit Tests
// Tests for DoS prevention via request body size validation
// =============================================================================

TEST_F(HttpQueryHandlerTest, ValidateRequestBodyWithinLimit) {
    HttpQueryHandler handler(nullptr);

    seastar::http::request req;
    req.content = R"json({"query":"avg:temperature()","startTime":1000,"endTime":2000})json";

    auto result = handler.validateRequest(req);
    EXPECT_EQ(result, nullptr) << "Valid small request should pass validation";
}

TEST_F(HttpQueryHandlerTest, ValidateRequestBodyExceedsLimit) {
    HttpQueryHandler handler(nullptr);

    seastar::http::request req;
    // Create a body larger than 1MB
    req.content = std::string(HttpQueryHandler::maxQueryBodySize() + 1, 'x');

    auto result = handler.validateRequest(req);
    ASSERT_NE(result, nullptr) << "Oversized request should fail validation";

    // Verify it returns an error about body size
    EXPECT_NE(result->_content.find("too large"), std::string::npos);
}

TEST_F(HttpQueryHandlerTest, ValidateRequestBodyExactlyAtLimit) {
    HttpQueryHandler handler(nullptr);

    seastar::http::request req;
    // Create a body exactly at the 1MB limit
    req.content = std::string(HttpQueryHandler::maxQueryBodySize(), 'x');

    auto result = handler.validateRequest(req);
    EXPECT_EQ(result, nullptr) << "Request body exactly at limit should pass validation";
}

TEST_F(HttpQueryHandlerTest, ValidateRequestEmptyBody) {
    HttpQueryHandler handler(nullptr);

    seastar::http::request req;
    req.content = "";

    auto result = handler.validateRequest(req);
    EXPECT_EQ(result, nullptr) << "Empty body should pass size validation (handled later by parser)";
}

// =============================================================================
// Content-Type Validation Tests
// Tests for Content-Type header validation
// =============================================================================

TEST_F(HttpQueryHandlerTest, ValidateRequestCorrectContentType) {
    HttpQueryHandler handler(nullptr);

    seastar::http::request req;
    req.content = R"json({"query":"avg:temperature()","startTime":1000,"endTime":2000})json";
    req._headers["Content-Type"] = "application/json";

    auto result = handler.validateRequest(req);
    EXPECT_EQ(result, nullptr) << "application/json Content-Type should pass validation";
}

TEST_F(HttpQueryHandlerTest, ValidateRequestContentTypeWithCharset) {
    HttpQueryHandler handler(nullptr);

    seastar::http::request req;
    req.content = R"json({"query":"avg:temperature()","startTime":1000,"endTime":2000})json";
    req._headers["Content-Type"] = "application/json; charset=utf-8";

    auto result = handler.validateRequest(req);
    EXPECT_EQ(result, nullptr) << "application/json with charset should pass validation";
}

TEST_F(HttpQueryHandlerTest, ValidateRequestNoContentType) {
    HttpQueryHandler handler(nullptr);

    seastar::http::request req;
    req.content = R"json({"query":"avg:temperature()","startTime":1000,"endTime":2000})json";
    // No Content-Type header set

    auto result = handler.validateRequest(req);
    EXPECT_EQ(result, nullptr) << "Missing Content-Type should pass validation (lenient)";
}

TEST_F(HttpQueryHandlerTest, ValidateRequestWrongContentType) {
    HttpQueryHandler handler(nullptr);

    seastar::http::request req;
    req.content = R"json({"query":"avg:temperature()","startTime":1000,"endTime":2000})json";
    req._headers["Content-Type"] = "text/html";

    auto result = handler.validateRequest(req);
    ASSERT_NE(result, nullptr) << "text/html Content-Type should fail validation";

    // Verify it returns an error mentioning content type
    EXPECT_NE(result->_content.find("Content-Type"), std::string::npos);
    EXPECT_NE(result->_content.find("application/json"), std::string::npos);
}

TEST_F(HttpQueryHandlerTest, ValidateRequestWrongContentTypePlainText) {
    HttpQueryHandler handler(nullptr);

    seastar::http::request req;
    req.content = R"json({"query":"avg:temperature()","startTime":1000,"endTime":2000})json";
    req._headers["Content-Type"] = "text/plain";

    auto result = handler.validateRequest(req);
    ASSERT_NE(result, nullptr) << "text/plain Content-Type should fail validation";
    EXPECT_NE(result->_content.find("Content-Type"), std::string::npos);
}

TEST_F(HttpQueryHandlerTest, ValidateRequestWrongContentTypeXml) {
    HttpQueryHandler handler(nullptr);

    seastar::http::request req;
    req.content = R"json({"query":"avg:temperature()","startTime":1000,"endTime":2000})json";
    req._headers["Content-Type"] = "application/xml";

    auto result = handler.validateRequest(req);
    ASSERT_NE(result, nullptr) << "application/xml Content-Type should fail validation";
}

TEST_F(HttpQueryHandlerTest, ValidateRequestBodySizeTakesPrecedenceOverContentType) {
    HttpQueryHandler handler(nullptr);

    seastar::http::request req;
    // Both oversized body AND wrong content type
    req.content = std::string(HttpQueryHandler::maxQueryBodySize() + 1, 'x');
    req._headers["Content-Type"] = "text/html";

    auto result = handler.validateRequest(req);
    ASSERT_NE(result, nullptr);
    // Body size check comes first, so the error should be about size
    EXPECT_NE(result->_content.find("too large"), std::string::npos);
}

// =============================================================================
// MAX_QUERY_BODY_SIZE Constant Tests
// =============================================================================

TEST_F(HttpQueryHandlerTest, MaxQueryBodySizeIs1MB) {
    EXPECT_EQ(HttpQueryHandler::maxQueryBodySize(), 1 * 1024 * 1024);
}