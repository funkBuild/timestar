#include "../../../lib/http/http_query_handler.hpp"

#include "../../../lib/query/aggregator.hpp"
#include "../../../lib/query/query_parser.hpp"

#include <glaze/glaze.hpp>

#include <gtest/gtest.h>

using namespace timestar;

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
    static constexpr auto value = object("query", &T::query, "startTime", &T::startTime, "endTime", &T::endTime,
                                         "aggregationInterval", &T::aggregationInterval);
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
    std::string error_code;
    std::string message;
    std::string error;
};

// Glaze meta declarations
template <>
struct glz::meta<GlazeFieldData> {
    using T = GlazeFieldData;
    static constexpr auto value = object("timestamps", &T::timestamps, "values", &T::values);
};

template <>
struct glz::meta<GlazeSeriesData> {
    using T = GlazeSeriesData;
    static constexpr auto value =
        object("measurement", &T::measurement, "groupTags", &T::groupTags, "fields", &T::fields);
};

template <>
struct glz::meta<GlazeStatistics> {
    using T = GlazeStatistics;
    static constexpr auto value = object("series_count", &T::series_count, "point_count", &T::point_count,
                                         "execution_time_ms", &T::execution_time_ms);
};

template <>
struct glz::meta<GlazeQueryResponse> {
    using T = GlazeQueryResponse;
    static constexpr auto value = object("status", &T::status, "series", &T::series, "statistics", &T::statistics);
};

template <>
struct glz::meta<GlazeErrorResponse> {
    using T = GlazeErrorResponse;
    static constexpr auto value =
        object("status", &T::status, "error_code", &T::error_code, "message", &T::message, "error", &T::error);
};

class HttpQueryHandlerTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}

    // Helper to create JSON string using Glaze
    std::string createJsonRequest(const std::string& query, const std::string& startTime, const std::string& endTime) {
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
    EXPECT_TRUE(std::find(seriesObj.groupTags.begin(), seriesObj.groupTags.end(), "location=office") !=
                seriesObj.groupTags.end());
    EXPECT_TRUE(std::find(seriesObj.groupTags.begin(), seriesObj.groupTags.end(), "sensor=temp-01") !=
                seriesObj.groupTags.end());

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

    std::string jsonStr =
        createJsonRequest("avg:temperature(value){location:office}", "1640995200000000000", "1641081599000000000");

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
    if (shardCount == 0)
        shardCount = 1;  // Test environment default
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

// =============================================================================
// Time Range Validation Tests
// Tests for startTime/endTime validation in parseQueryRequest
// =============================================================================

TEST_F(HttpQueryHandlerTest, ParseQueryRequestStartTimeEqualsEndTimeThrows) {
    HttpQueryHandler handler(nullptr);

    GlazeQueryRequest glazeReq;
    glazeReq.query = "avg:temperature()";
    glazeReq.startTime = uint64_t(1000000000);
    glazeReq.endTime = uint64_t(1000000000);

    EXPECT_THROW(handler.parseQueryRequest(glazeReq), QueryParseException);
}

TEST_F(HttpQueryHandlerTest, ParseQueryRequestStartTimeEqualsEndTimeErrorMessage) {
    HttpQueryHandler handler(nullptr);

    GlazeQueryRequest glazeReq;
    glazeReq.query = "avg:temperature()";
    glazeReq.startTime = uint64_t(5000000000);
    glazeReq.endTime = uint64_t(5000000000);

    try {
        handler.parseQueryRequest(glazeReq);
        FAIL() << "Expected QueryParseException";
    } catch (const QueryParseException& e) {
        std::string msg = e.what();
        EXPECT_NE(msg.find("startTime must be less than endTime"), std::string::npos)
            << "Error message should mention time ordering, got: " << msg;
    }
}

TEST_F(HttpQueryHandlerTest, ParseQueryRequestStartTimeGreaterThanEndTimeThrows) {
    HttpQueryHandler handler(nullptr);

    GlazeQueryRequest glazeReq;
    glazeReq.query = "avg:temperature()";
    glazeReq.startTime = uint64_t(2000000000);
    glazeReq.endTime = uint64_t(1000000000);

    EXPECT_THROW(handler.parseQueryRequest(glazeReq), QueryParseException);
}

TEST_F(HttpQueryHandlerTest, ParseQueryRequestStartTimeGreaterThanEndTimeStringTimestamps) {
    HttpQueryHandler handler(nullptr);

    GlazeQueryRequest glazeReq;
    glazeReq.query = "avg:temperature()";
    glazeReq.startTime = std::string("2000000000");
    glazeReq.endTime = std::string("1000000000");

    EXPECT_THROW(handler.parseQueryRequest(glazeReq), QueryParseException);
}

TEST_F(HttpQueryHandlerTest, ParseQueryRequestValidTimeRangeSucceeds) {
    HttpQueryHandler handler(nullptr);

    GlazeQueryRequest glazeReq;
    glazeReq.query = "avg:temperature()";
    glazeReq.startTime = uint64_t(1000000000);
    glazeReq.endTime = uint64_t(2000000000);

    QueryRequest request = handler.parseQueryRequest(glazeReq);
    EXPECT_EQ(request.startTime, 1000000000ULL);
    EXPECT_EQ(request.endTime, 2000000000ULL);
}

TEST_F(HttpQueryHandlerTest, ParseQueryRequestWithStringInterval) {
    HttpQueryHandler handler(nullptr);

    GlazeQueryRequest glazeReq;
    glazeReq.query = "avg:temperature()";
    glazeReq.startTime = uint64_t(1000000000);
    glazeReq.endTime = uint64_t(2000000000);
    glazeReq.aggregationInterval = std::string("5m");

    QueryRequest request = handler.parseQueryRequest(glazeReq);
    EXPECT_EQ(request.aggregationInterval, 5ULL * 60 * 1000000000);
}

TEST_F(HttpQueryHandlerTest, ParseQueryRequestWithNumericInterval) {
    HttpQueryHandler handler(nullptr);

    GlazeQueryRequest glazeReq;
    glazeReq.query = "avg:temperature()";
    glazeReq.startTime = uint64_t(1000000000);
    glazeReq.endTime = uint64_t(2000000000);
    glazeReq.aggregationInterval = uint64_t(300000000000);

    QueryRequest request = handler.parseQueryRequest(glazeReq);
    EXPECT_EQ(request.aggregationInterval, 300000000000ULL);
}

TEST_F(HttpQueryHandlerTest, ParseQueryRequestNoInterval) {
    HttpQueryHandler handler(nullptr);

    GlazeQueryRequest glazeReq;
    glazeReq.query = "avg:temperature()";
    glazeReq.startTime = uint64_t(1000000000);
    glazeReq.endTime = uint64_t(2000000000);
    // No aggregationInterval set

    QueryRequest request = handler.parseQueryRequest(glazeReq);
    EXPECT_EQ(request.aggregationInterval, 0ULL);
}

// =============================================================================
// finalizeSingleShardPartials Tests
// Tests the 4 code paths for converting partial aggregation results into
// the final QueryResponse without the full merge pipeline.
// =============================================================================

// Path 1: Bucketed results (bucketStates populated)
TEST_F(HttpQueryHandlerTest, FinalizeSingleShardPartialsBucketed) {
    std::vector<PartialAggregationResult> partials;

    PartialAggregationResult partial;
    partial.measurement = "cpu";
    partial.fieldName = "usage";
    partial.groupKey = "cpu\0usage";
    partial.cachedTags = {{"host", "server-01"}};

    // Add 3 buckets with pre-computed states
    AggregationState state1;
    state1.sum = 100.0;
    state1.count = 5;
    partial.bucketStates[1000] = state1;

    AggregationState state2;
    state2.sum = 200.0;
    state2.count = 5;
    partial.bucketStates[2000] = state2;

    AggregationState state3;
    state3.sum = 150.0;
    state3.count = 5;
    partial.bucketStates[3000] = state3;

    partials.push_back(std::move(partial));

    QueryResponse response;
    response.success = true;
    HttpQueryHandler::finalizeSingleShardPartials(partials, AggregationMethod::AVG, response);

    ASSERT_EQ(response.series.size(), 1);
    EXPECT_EQ(response.series[0].measurement, "cpu");
    EXPECT_EQ(response.series[0].tags.at("host"), "server-01");

    auto& field = response.series[0].fields.at("usage");
    auto& timestamps = field.first;
    auto& values = std::get<std::vector<double>>(field.second);

    // Buckets should be sorted by timestamp
    ASSERT_EQ(timestamps.size(), 3);
    EXPECT_EQ(timestamps[0], 1000);
    EXPECT_EQ(timestamps[1], 2000);
    EXPECT_EQ(timestamps[2], 3000);

    // AVG = sum/count
    EXPECT_DOUBLE_EQ(values[0], 20.0);
    EXPECT_DOUBLE_EQ(values[1], 40.0);
    EXPECT_DOUBLE_EQ(values[2], 30.0);
}

// Path 2: Collapsed state (single pre-folded AggregationState)
TEST_F(HttpQueryHandlerTest, FinalizeSingleShardPartialsCollapsedState) {
    std::vector<PartialAggregationResult> partials;

    PartialAggregationResult partial;
    partial.measurement = "temperature";
    partial.fieldName = "value";
    partial.groupKey = "temperature\0value";
    partial.cachedTags = {{"location", "us-west"}};

    AggregationState state;
    state.count = 10;
    state.latest = 42.5;
    state.latestTimestamp = 5000;
    state.first = 38.0;
    state.firstTimestamp = 1000;
    partial.collapsedState = state;

    partials.push_back(std::move(partial));

    QueryResponse response;
    response.success = true;
    HttpQueryHandler::finalizeSingleShardPartials(partials, AggregationMethod::LATEST, response);

    ASSERT_EQ(response.series.size(), 1);
    EXPECT_EQ(response.series[0].measurement, "temperature");
    EXPECT_EQ(response.series[0].tags.at("location"), "us-west");

    auto& field = response.series[0].fields.at("value");
    auto& timestamps = field.first;
    auto& values = std::get<std::vector<double>>(field.second);

    ASSERT_EQ(timestamps.size(), 1);
    EXPECT_EQ(timestamps[0], 5000);  // LATEST uses latestTimestamp
    EXPECT_DOUBLE_EQ(values[0], 42.5);
}

// Path 2 variant: Collapsed state with FIRST method
TEST_F(HttpQueryHandlerTest, FinalizeSingleShardPartialsCollapsedStateFirst) {
    std::vector<PartialAggregationResult> partials;

    PartialAggregationResult partial;
    partial.measurement = "temperature";
    partial.fieldName = "value";
    partial.groupKey = "temperature\0value";

    AggregationState state;
    state.count = 10;
    state.latest = 42.5;
    state.latestTimestamp = 5000;
    state.first = 38.0;
    state.firstTimestamp = 1000;
    partial.collapsedState = state;

    partials.push_back(std::move(partial));

    QueryResponse response;
    response.success = true;
    HttpQueryHandler::finalizeSingleShardPartials(partials, AggregationMethod::FIRST, response);

    ASSERT_EQ(response.series.size(), 1);
    auto& field = response.series[0].fields.at("value");
    auto& timestamps = field.first;
    auto& values = std::get<std::vector<double>>(field.second);

    ASSERT_EQ(timestamps.size(), 1);
    EXPECT_EQ(timestamps[0], 1000);  // FIRST uses firstTimestamp
    EXPECT_DOUBLE_EQ(values[0], 38.0);
}

// Path 2 edge: Collapsed state with count == 0 produces no series
TEST_F(HttpQueryHandlerTest, FinalizeSingleShardPartialsCollapsedStateEmpty) {
    std::vector<PartialAggregationResult> partials;

    PartialAggregationResult partial;
    partial.measurement = "temperature";
    partial.fieldName = "value";
    partial.groupKey = "temperature\0value";

    AggregationState state;
    state.count = 0;
    partial.collapsedState = state;

    partials.push_back(std::move(partial));

    QueryResponse response;
    response.success = true;
    HttpQueryHandler::finalizeSingleShardPartials(partials, AggregationMethod::AVG, response);

    // count == 0 means the collapsed state is empty; should produce no series
    EXPECT_EQ(response.series.size(), 0);
}

// Path 3: sortedValues populated (raw values from pushdown)
TEST_F(HttpQueryHandlerTest, FinalizeSingleShardPartialsSortedValues) {
    std::vector<PartialAggregationResult> partials;

    PartialAggregationResult partial;
    partial.measurement = "disk";
    partial.fieldName = "iops";
    partial.groupKey = "disk\0iops";
    partial.cachedTags = {{"device", "sda"}};
    partial.sortedTimestamps = {100, 200, 300, 400};
    partial.sortedValues = {10.0, 20.0, 30.0, 40.0};

    partials.push_back(std::move(partial));

    QueryResponse response;
    response.success = true;
    HttpQueryHandler::finalizeSingleShardPartials(partials, AggregationMethod::AVG, response);

    ASSERT_EQ(response.series.size(), 1);
    EXPECT_EQ(response.series[0].measurement, "disk");

    auto& field = response.series[0].fields.at("iops");
    auto& timestamps = field.first;
    auto& values = std::get<std::vector<double>>(field.second);

    ASSERT_EQ(timestamps.size(), 4);
    EXPECT_EQ(timestamps[0], 100);
    EXPECT_EQ(timestamps[3], 400);
    EXPECT_DOUBLE_EQ(values[0], 10.0);
    EXPECT_DOUBLE_EQ(values[3], 40.0);
}

// Path 3 with COUNT: raw values replaced with 1.0 per point
TEST_F(HttpQueryHandlerTest, FinalizeSingleShardPartialsSortedValuesCount) {
    std::vector<PartialAggregationResult> partials;

    PartialAggregationResult partial;
    partial.measurement = "requests";
    partial.fieldName = "count";
    partial.groupKey = "requests\0count";
    partial.sortedTimestamps = {100, 200, 300};
    partial.sortedValues = {5.0, 10.0, 15.0};

    partials.push_back(std::move(partial));

    QueryResponse response;
    response.success = true;
    HttpQueryHandler::finalizeSingleShardPartials(partials, AggregationMethod::COUNT, response);

    ASSERT_EQ(response.series.size(), 1);
    auto& field = response.series[0].fields.at("count");
    auto& values = std::get<std::vector<double>>(field.second);

    // COUNT replaces all values with 1.0
    ASSERT_EQ(values.size(), 3);
    EXPECT_DOUBLE_EQ(values[0], 1.0);
    EXPECT_DOUBLE_EQ(values[1], 1.0);
    EXPECT_DOUBLE_EQ(values[2], 1.0);
}

// Path 4: sortedStates populated (fallback aggregation)
TEST_F(HttpQueryHandlerTest, FinalizeSingleShardPartialsSortedStates) {
    std::vector<PartialAggregationResult> partials;

    PartialAggregationResult partial;
    partial.measurement = "memory";
    partial.fieldName = "usage_pct";
    partial.groupKey = "memory\0usage_pct";
    partial.cachedTags = {{"host", "web-01"}};

    // Build sorted states with known values
    AggregationState s1;
    s1.count = 1;
    s1.sum = 75.0;
    s1.min = 75.0;
    s1.max = 75.0;

    AggregationState s2;
    s2.count = 1;
    s2.sum = 82.0;
    s2.min = 82.0;
    s2.max = 82.0;

    partial.sortedTimestamps = {1000, 2000};
    partial.sortedStates = {s1, s2};

    partials.push_back(std::move(partial));

    QueryResponse response;
    response.success = true;
    HttpQueryHandler::finalizeSingleShardPartials(partials, AggregationMethod::SUM, response);

    ASSERT_EQ(response.series.size(), 1);
    EXPECT_EQ(response.series[0].measurement, "memory");
    EXPECT_EQ(response.series[0].tags.at("host"), "web-01");

    auto& field = response.series[0].fields.at("usage_pct");
    auto& timestamps = field.first;
    auto& values = std::get<std::vector<double>>(field.second);

    ASSERT_EQ(timestamps.size(), 2);
    EXPECT_EQ(timestamps[0], 1000);
    EXPECT_EQ(timestamps[1], 2000);
    // SUM: getValue returns sum
    EXPECT_DOUBLE_EQ(values[0], 75.0);
    EXPECT_DOUBLE_EQ(values[1], 82.0);
}

// Edge case: Empty partials vector produces no series
TEST_F(HttpQueryHandlerTest, FinalizeSingleShardPartialsEmpty) {
    std::vector<PartialAggregationResult> partials;

    QueryResponse response;
    response.success = true;
    HttpQueryHandler::finalizeSingleShardPartials(partials, AggregationMethod::AVG, response);

    EXPECT_EQ(response.series.size(), 0);
}

// Edge case: Partial with no data in any path is skipped
TEST_F(HttpQueryHandlerTest, FinalizeSingleShardPartialsSkipsEmptyPartial) {
    std::vector<PartialAggregationResult> partials;

    // First partial: completely empty (no bucketStates, no collapsedState,
    // no sortedValues, no sortedStates) => should be skipped
    PartialAggregationResult emptyPartial;
    emptyPartial.measurement = "empty";
    emptyPartial.fieldName = "field";
    emptyPartial.groupKey = "empty\0field";
    partials.push_back(std::move(emptyPartial));

    // Second partial: has actual data
    PartialAggregationResult dataPartial;
    dataPartial.measurement = "cpu";
    dataPartial.fieldName = "idle";
    dataPartial.groupKey = "cpu\0idle";
    dataPartial.sortedTimestamps = {500};
    dataPartial.sortedValues = {99.5};
    partials.push_back(std::move(dataPartial));

    QueryResponse response;
    response.success = true;
    HttpQueryHandler::finalizeSingleShardPartials(partials, AggregationMethod::AVG, response);

    // Only the data-bearing partial should produce a series
    ASSERT_EQ(response.series.size(), 1);
    EXPECT_EQ(response.series[0].measurement, "cpu");
}

// Multiple partials from different series
TEST_F(HttpQueryHandlerTest, FinalizeSingleShardPartialsMultipleSeries) {
    std::vector<PartialAggregationResult> partials;

    // Series 1: collapsed state
    PartialAggregationResult p1;
    p1.measurement = "temp";
    p1.fieldName = "celsius";
    p1.groupKey = "temp\0location=east\0celsius";
    p1.cachedTags = {{"location", "east"}};
    AggregationState s1;
    s1.count = 5;
    s1.max = 35.0;
    p1.collapsedState = s1;
    partials.push_back(std::move(p1));

    // Series 2: raw values
    PartialAggregationResult p2;
    p2.measurement = "temp";
    p2.fieldName = "celsius";
    p2.groupKey = "temp\0location=west\0celsius";
    p2.cachedTags = {{"location", "west"}};
    p2.sortedTimestamps = {100, 200};
    p2.sortedValues = {22.0, 23.0};
    partials.push_back(std::move(p2));

    QueryResponse response;
    response.success = true;
    HttpQueryHandler::finalizeSingleShardPartials(partials, AggregationMethod::MAX, response);

    ASSERT_EQ(response.series.size(), 2);

    // Both series should have the correct measurement
    EXPECT_EQ(response.series[0].measurement, "temp");
    EXPECT_EQ(response.series[1].measurement, "temp");

    // Verify different tags distinguish them
    bool foundEast = false, foundWest = false;
    for (const auto& s : response.series) {
        if (s.tags.count("location") && s.tags.at("location") == "east")
            foundEast = true;
        if (s.tags.count("location") && s.tags.at("location") == "west")
            foundWest = true;
    }
    EXPECT_TRUE(foundEast);
    EXPECT_TRUE(foundWest);
}

// Bucketed results should be sorted by timestamp regardless of insertion order
TEST_F(HttpQueryHandlerTest, FinalizeSingleShardPartialsBucketsSortedByTimestamp) {
    std::vector<PartialAggregationResult> partials;

    PartialAggregationResult partial;
    partial.measurement = "net";
    partial.fieldName = "bytes";
    partial.groupKey = "net\0bytes";

    // Insert buckets in reverse order
    AggregationState s;
    s.count = 1;

    s.sum = 300.0;
    partial.bucketStates[3000] = s;
    s.sum = 100.0;
    partial.bucketStates[1000] = s;
    s.sum = 200.0;
    partial.bucketStates[2000] = s;

    partials.push_back(std::move(partial));

    QueryResponse response;
    response.success = true;
    HttpQueryHandler::finalizeSingleShardPartials(partials, AggregationMethod::SUM, response);

    ASSERT_EQ(response.series.size(), 1);
    auto& field = response.series[0].fields.at("bytes");
    auto& timestamps = field.first;
    auto& values = std::get<std::vector<double>>(field.second);

    // Must be sorted ascending
    ASSERT_EQ(timestamps.size(), 3);
    EXPECT_EQ(timestamps[0], 1000);
    EXPECT_EQ(timestamps[1], 2000);
    EXPECT_EQ(timestamps[2], 3000);
    EXPECT_DOUBLE_EQ(values[0], 100.0);
    EXPECT_DOUBLE_EQ(values[1], 200.0);
    EXPECT_DOUBLE_EQ(values[2], 300.0);
}