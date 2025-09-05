#include <gtest/gtest.h>
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/query/query_parser.hpp"
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
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

class HttpQueryHandlerTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
    
    // Helper to convert RapidJSON document to GlazeQueryRequest
    GlazeQueryRequest rapidJsonToGlaze(const rapidjson::Document& doc) {
        GlazeQueryRequest req;
        
        if (doc.HasMember("query") && doc["query"].IsString()) {
            req.query = doc["query"].GetString();
        }
        
        if (doc.HasMember("startTime")) {
            if (doc["startTime"].IsString()) {
                req.startTime = std::string(doc["startTime"].GetString());
            } else if (doc["startTime"].IsUint64()) {
                req.startTime = doc["startTime"].GetUint64();
            }
        }
        
        if (doc.HasMember("endTime")) {
            if (doc["endTime"].IsString()) {
                req.endTime = std::string(doc["endTime"].GetString());
            } else if (doc["endTime"].IsUint64()) {
                req.endTime = doc["endTime"].GetUint64();
            }
        }
        
        if (doc.HasMember("aggregationInterval")) {
            if (doc["aggregationInterval"].IsString()) {
                req.aggregationInterval = std::string(doc["aggregationInterval"].GetString());
            } else if (doc["aggregationInterval"].IsUint64()) {
                req.aggregationInterval = doc["aggregationInterval"].GetUint64();
            }
        } else if (doc.HasMember("interval")) {  // Support old field name
            if (doc["interval"].IsString()) {
                req.aggregationInterval = std::string(doc["interval"].GetString());
            } else if (doc["interval"].IsUint64()) {
                req.aggregationInterval = doc["interval"].GetUint64();
            }
        }
        
        return req;
    }
    
    // Helper to create JSON string
    std::string createJsonRequest(const std::string& query, 
                                 const std::string& startTime,
                                 const std::string& endTime) {
        rapidjson::Document doc;
        doc.SetObject();
        auto& allocator = doc.GetAllocator();
        
        doc.AddMember("query", rapidjson::Value(query.c_str(), allocator), allocator);
        doc.AddMember("startTime", rapidjson::Value(startTime.c_str(), allocator), allocator);
        doc.AddMember("endTime", rapidjson::Value(endTime.c_str(), allocator), allocator);
        
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);
        
        return buffer.GetString();
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
    
    // Parse the JSON to verify structure
    rapidjson::Document doc;
    doc.Parse(json.c_str());
    
    ASSERT_FALSE(doc.HasParseError());
    EXPECT_STREQ(doc["status"].GetString(), "success");
    EXPECT_TRUE(doc["series"].IsArray());
    EXPECT_EQ(doc["series"].Size(), 0);
    EXPECT_TRUE(doc["statistics"].IsObject());
    EXPECT_EQ(doc["statistics"]["series_count"].GetUint64(), 0);
    EXPECT_EQ(doc["statistics"]["point_count"].GetUint64(), 0);
    EXPECT_DOUBLE_EQ(doc["statistics"]["execution_time_ms"].GetDouble(), 5.5);
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
    
    // Parse and verify
    rapidjson::Document doc;
    doc.Parse(json.c_str());
    
    ASSERT_FALSE(doc.HasParseError());
    EXPECT_STREQ(doc["status"].GetString(), "success");
    EXPECT_EQ(doc["series"].Size(), 1);
    
    const auto& seriesObj = doc["series"][0];
    EXPECT_STREQ(seriesObj["measurement"].GetString(), "temperature");
    EXPECT_STREQ(seriesObj["tags"]["location"].GetString(), "office");
    EXPECT_STREQ(seriesObj["tags"]["sensor"].GetString(), "temp-01");
    
    const auto& fieldData = seriesObj["fields"]["value"];
    EXPECT_EQ(fieldData["timestamps"].Size(), 3);
    EXPECT_EQ(fieldData["values"].Size(), 3);
    EXPECT_EQ(fieldData["timestamps"][0].GetUint64(), 1000000000);
    EXPECT_DOUBLE_EQ(fieldData["values"][0].GetDouble(), 20.5);
}

TEST_F(HttpQueryHandlerTest, FormatErrorResponse) {
    HttpQueryHandler handler(nullptr);
    
    std::string json = handler.createErrorResponse("INVALID_QUERY", "Missing measurement");
    
    rapidjson::Document doc;
    doc.Parse(json.c_str());
    
    ASSERT_FALSE(doc.HasParseError());
    EXPECT_STREQ(doc["status"].GetString(), "error");
    EXPECT_TRUE(doc["error"].IsObject());
    EXPECT_STREQ(doc["error"]["code"].GetString(), "INVALID_QUERY");
    EXPECT_STREQ(doc["error"]["message"].GetString(), "Missing measurement");
}

// Test request parsing
TEST_F(HttpQueryHandlerTest, ParseValidQueryRequest) {
    HttpQueryHandler handler(nullptr);
    
    std::string jsonStr = createJsonRequest(
        "avg:temperature(value){location:office}",
        "01-01-2024 00:00:00",
        "02-01-2024 00:00:00"
    );
    
    rapidjson::Document doc;
    doc.Parse(jsonStr.c_str());
    
    QueryRequest request = handler.parseQueryRequest(rapidJsonToGlaze(doc));
    
    EXPECT_EQ(request.aggregation, AggregationMethod::AVG);
    EXPECT_EQ(request.measurement, "temperature");
    EXPECT_EQ(request.fields.size(), 1);
    EXPECT_EQ(request.fields[0], "value");
    EXPECT_EQ(request.scopes.at("location"), "office");
}

TEST_F(HttpQueryHandlerTest, ParseQueryRequestMissingField) {
    HttpQueryHandler handler(nullptr);
    
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    // Missing query field
    doc.AddMember("startTime", "01-01-2024 00:00:00", allocator);
    doc.AddMember("endTime", "02-01-2024 00:00:00", allocator);
    
    EXPECT_THROW(handler.parseQueryRequest(rapidJsonToGlaze(doc)), QueryParseException);
}

TEST_F(HttpQueryHandlerTest, ParseQueryRequestInvalidTypes) {
    HttpQueryHandler handler(nullptr);
    
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    // Query as number instead of string
    doc.AddMember("query", 123, allocator);
    doc.AddMember("startTime", "01-01-2024 00:00:00", allocator);
    doc.AddMember("endTime", "02-01-2024 00:00:00", allocator);
    
    EXPECT_THROW(handler.parseQueryRequest(rapidJsonToGlaze(doc)), QueryParseException);
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
    for (unsigned i = 0; i < seastar::smp::count; ++i) {
        EXPECT_TRUE(std::find(shards.begin(), shards.end(), i) != shards.end());
    }
}

// Test result merging
TEST_F(HttpQueryHandlerTest, MergeResults) {
    HttpQueryHandler handler(nullptr);
    
    std::vector<std::vector<SeriesResult>> shardResults;
    
    // Shard 1 results
    std::vector<SeriesResult> shard1;
    SeriesResult s1;
    s1.measurement = "temp";
    s1.tags["location"] = "room1";
    shard1.push_back(s1);
    
    // Shard 2 results
    std::vector<SeriesResult> shard2;
    SeriesResult s2;
    s2.measurement = "temp";
    s2.tags["location"] = "room2";
    shard2.push_back(s2);
    
    shardResults.push_back(shard1);
    shardResults.push_back(shard2);
    
    auto merged = handler.mergeResults(shardResults);
    
    EXPECT_EQ(merged.size(), 2);
    EXPECT_EQ(merged[0].tags.at("location"), "room1");
    EXPECT_EQ(merged[1].tags.at("location"), "room2");
}

// Test aggregation application
TEST_F(HttpQueryHandlerTest, ApplyAggregationAverage) {
    HttpQueryHandler handler(nullptr);
    
    std::vector<SeriesResult> results;
    SeriesResult series;
    series.measurement = "test";
    
    std::vector<uint64_t> timestamps = {1000, 2000, 3000};
    std::vector<double> values = {10.0, 20.0, 30.0};
    series.fields["value"] = std::make_pair(timestamps, FieldValues(values));
    results.push_back(series);
    
    QueryRequest request;
    request.aggregation = AggregationMethod::AVG;
    request.aggregationInterval = 0; // No time bucketing - aggregate all into one
    
    // With interval=0, aggregation combines all points into one average
    handler.applyAggregation(results, request);
    
    EXPECT_EQ(results.size(), 1);
    auto& fieldData = results[0].fields["value"];
    if (std::holds_alternative<std::vector<double>>(fieldData.second)) {
        auto& aggValues = std::get<std::vector<double>>(fieldData.second);
        EXPECT_EQ(aggValues.size(), 1); // One aggregated point
        EXPECT_DOUBLE_EQ(aggValues[0], 20.0); // Average of 10,20,30
    }
}

// Test group by application
TEST_F(HttpQueryHandlerTest, ApplyGroupByNoGroups) {
    HttpQueryHandler handler(nullptr);
    
    std::vector<SeriesResult> results;
    SeriesResult series;
    series.measurement = "test";
    results.push_back(series);
    
    QueryRequest request;
    // No group by tags
    
    auto grouped = handler.applyGroupBy(results, request);
    
    EXPECT_EQ(grouped.size(), 1);
}

TEST_F(HttpQueryHandlerTest, ApplyGroupByWithGroups) {
    HttpQueryHandler handler(nullptr);
    
    std::vector<SeriesResult> results;
    SeriesResult s1, s2;
    s1.measurement = "test";
    s1.tags["location"] = "room1";
    s2.measurement = "test";
    s2.tags["location"] = "room2";
    results.push_back(s1);
    results.push_back(s2);
    
    QueryRequest request;
    request.groupByTags.push_back("location");
    
    auto grouped = handler.applyGroupBy(results, request);
    
    // Currently returns unchanged
    EXPECT_EQ(grouped.size(), 2);
}

// Integration test for query execution
// NOTE: This test is commented out as it requires Seastar runtime
// TEST_F(HttpQueryHandlerTest, ExecuteQueryMock) {
//     HttpQueryHandler handler(nullptr);
//     
//     QueryRequest request;
//     request.measurement = "temperature";
//     request.fields.push_back("value");
//     request.scopes["location"] = "office";
//     request.startTime = 1000000000;
//     request.endTime = 2000000000;
//     request.aggregation = AggregationMethod::AVG;
//     
//     // Execute query (will use mock data)
//     auto future = handler.executeQuery(request);
//     
//     // In a real test with Seastar, we would co_await this
//     // For now, we can't test the async execution without Seastar runtime
// }

// Test response with multiple fields
TEST_F(HttpQueryHandlerTest, FormatResponseMultipleFields) {
    HttpQueryHandler handler(nullptr);
    
    QueryResponse response;
    response.success = true;
    
    SeriesResult series;
    series.measurement = "weather";
    series.tags["station"] = "LAX";
    
    std::vector<uint64_t> timestamps = {1000, 2000};
    std::vector<double> tempValues = {25.5, 26.0};
    std::vector<double> humidityValues = {65.0, 67.0};
    
    series.fields["temperature"] = std::make_pair(timestamps, tempValues);
    series.fields["humidity"] = std::make_pair(timestamps, humidityValues);
    
    response.series.push_back(series);
    response.statistics.seriesCount = 1;
    response.statistics.pointCount = 4;  // 2 timestamps × 2 fields
    
    std::string json = handler.formatQueryResponse(response);
    
    rapidjson::Document doc;
    doc.Parse(json.c_str());
    
    ASSERT_FALSE(doc.HasParseError());
    
    const auto& fields = doc["series"][0]["fields"];
    EXPECT_TRUE(fields.HasMember("temperature"));
    EXPECT_TRUE(fields.HasMember("humidity"));
    EXPECT_EQ(fields["temperature"]["values"].Size(), 2);
    EXPECT_EQ(fields["humidity"]["values"].Size(), 2);
}

// Test aggregationInterval parsing
TEST_F(HttpQueryHandlerTest, ParseAggregationIntervalNumeric) {
    HttpQueryHandler handler(nullptr);
    
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    doc.AddMember("query", "avg:temperature(value)", allocator);
    doc.AddMember("startTime", rapidjson::Value(1704067200000000000), allocator);
    doc.AddMember("endTime", rapidjson::Value(1704153600000000000), allocator);
    doc.AddMember("aggregationInterval", rapidjson::Value(300000000000), allocator); // 5 minutes in nanoseconds
    
    QueryRequest request = handler.parseQueryRequest(rapidJsonToGlaze(doc));
    
    EXPECT_EQ(request.aggregationInterval, 300000000000ULL);
}

TEST_F(HttpQueryHandlerTest, ParseAggregationIntervalString) {
    HttpQueryHandler handler(nullptr);
    
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    doc.AddMember("query", "avg:temperature(value)", allocator);
    doc.AddMember("startTime", rapidjson::Value(1704067200000000000), allocator);
    doc.AddMember("endTime", rapidjson::Value(1704153600000000000), allocator);
    doc.AddMember("aggregationInterval", "5m", allocator);
    
    QueryRequest request = handler.parseQueryRequest(rapidJsonToGlaze(doc));
    
    EXPECT_EQ(request.aggregationInterval, 300000000000ULL); // 5 minutes in nanoseconds
}

TEST_F(HttpQueryHandlerTest, ParseIntervalBackwardCompatibility) {
    HttpQueryHandler handler(nullptr);
    
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    doc.AddMember("query", "avg:temperature(value)", allocator);
    doc.AddMember("startTime", rapidjson::Value(1704067200000000000), allocator);
    doc.AddMember("endTime", rapidjson::Value(1704153600000000000), allocator);
    doc.AddMember("interval", "1h", allocator); // Using old field name
    
    QueryRequest request = handler.parseQueryRequest(rapidJsonToGlaze(doc));
    
    EXPECT_EQ(request.aggregationInterval, 3600000000000ULL); // 1 hour in nanoseconds
}

TEST_F(HttpQueryHandlerTest, ParseIntervalUnits) {
    HttpQueryHandler handler(nullptr);
    
    // Test nanoseconds
    EXPECT_EQ(HttpQueryHandler::parseInterval("100ns"), 100ULL);
    
    // Test microseconds
    EXPECT_EQ(HttpQueryHandler::parseInterval("500us"), 500000ULL);
    EXPECT_EQ(HttpQueryHandler::parseInterval("500µs"), 500000ULL);
    
    // Test milliseconds
    EXPECT_EQ(HttpQueryHandler::parseInterval("100ms"), 100000000ULL);
    
    // Test seconds
    EXPECT_EQ(HttpQueryHandler::parseInterval("30s"), 30000000000ULL);
    
    // Test minutes
    EXPECT_EQ(HttpQueryHandler::parseInterval("5m"), 300000000000ULL);
    
    // Test hours
    EXPECT_EQ(HttpQueryHandler::parseInterval("2h"), 7200000000000ULL);
    
    // Test days
    EXPECT_EQ(HttpQueryHandler::parseInterval("1d"), 86400000000000ULL);
    
    // Test decimal values
    EXPECT_EQ(HttpQueryHandler::parseInterval("1.5s"), 1500000000ULL);
    EXPECT_EQ(HttpQueryHandler::parseInterval("0.5m"), 30000000000ULL);
}

TEST_F(HttpQueryHandlerTest, ParseIntervalInvalid) {
    HttpQueryHandler handler(nullptr);
    
    // Invalid formats
    EXPECT_THROW(HttpQueryHandler::parseInterval(""), QueryParseException);
    EXPECT_THROW(HttpQueryHandler::parseInterval("abc"), QueryParseException);
    EXPECT_THROW(HttpQueryHandler::parseInterval("5"), QueryParseException);
    EXPECT_THROW(HttpQueryHandler::parseInterval("5x"), QueryParseException);
}