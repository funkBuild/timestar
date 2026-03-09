#include <gtest/gtest.h>
#include "derived_query_executor.hpp"
#include "derived_query.hpp"
#include <glaze/glaze.hpp>

using namespace timestar;

// Test-local Glaze types for parsing responses (mirrors library types)
struct TestGlazeDerivedQueryRequest {
    std::map<std::string, std::string> queries;
    std::string formula;
    uint64_t startTime = 0;
    uint64_t endTime = 0;
    std::string aggregationInterval;
};

struct TestGlazeDerivedQueryResponse {
    std::string status;
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    std::string formula;

    struct Statistics {
        size_t pointCount = 0;
        double executionTimeMs = 0.0;
        size_t subQueriesExecuted = 0;
        size_t pointsDroppedDueToAlignment = 0;
    } statistics;

    struct Error {
        std::string code;
        std::string message;
    } error;
};

// Glaze meta templates for test types
template <>
struct glz::meta<TestGlazeDerivedQueryRequest> {
    using T = TestGlazeDerivedQueryRequest;
    static constexpr auto value = object(
        "queries", &T::queries,
        "formula", &T::formula,
        "startTime", &T::startTime,
        "endTime", &T::endTime,
        "aggregationInterval", &T::aggregationInterval
    );
};

template <>
struct glz::meta<TestGlazeDerivedQueryResponse::Statistics> {
    using T = TestGlazeDerivedQueryResponse::Statistics;
    static constexpr auto value = object(
        "point_count", &T::pointCount,
        "execution_time_ms", &T::executionTimeMs,
        "sub_queries_executed", &T::subQueriesExecuted,
        "points_dropped_due_to_alignment", &T::pointsDroppedDueToAlignment
    );
};

template <>
struct glz::meta<TestGlazeDerivedQueryResponse::Error> {
    using T = TestGlazeDerivedQueryResponse::Error;
    static constexpr auto value = object(
        "code", &T::code,
        "message", &T::message
    );
};

template <>
struct glz::meta<TestGlazeDerivedQueryResponse> {
    using T = TestGlazeDerivedQueryResponse;
    static constexpr auto value = object(
        "status", &T::status,
        "timestamps", &T::timestamps,
        "values", &T::values,
        "formula", &T::formula,
        "statistics", &T::statistics,
        "error", &T::error
    );
};

class DerivedQueryExecutorTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// ==================== JSON Parsing Tests ====================

TEST_F(DerivedQueryExecutorTest, ParseValidJsonRequest) {
    std::string json = R"json({
        "queries": {
            "a": "avg:cpu(usage){host:server1}",
            "b": "avg:memory(used){host:server1}"
        },
        "formula": "a / b * 100",
        "startTime": 1704067200000000000,
        "endTime": 1704153600000000000,
        "aggregationInterval": "5m"
    })json";

    TestGlazeDerivedQueryRequest request;
    auto result = glz::read_json(request, json);

    EXPECT_FALSE(result);  // No error
    EXPECT_EQ(request.queries.size(), 2);
    EXPECT_EQ(request.queries["a"], "avg:cpu(usage){host:server1}");
    EXPECT_EQ(request.queries["b"], "avg:memory(used){host:server1}");
    EXPECT_EQ(request.formula, "a / b * 100");
    EXPECT_EQ(request.startTime, 1704067200000000000ULL);
    EXPECT_EQ(request.endTime, 1704153600000000000ULL);
    EXPECT_EQ(request.aggregationInterval, "5m");
}

TEST_F(DerivedQueryExecutorTest, ParseMinimalJsonRequest) {
    std::string json = R"json({
        "queries": {
            "x": "sum:requests(count)"
        },
        "formula": "x"
    })json";

    TestGlazeDerivedQueryRequest request;
    auto result = glz::read_json(request, json);

    EXPECT_FALSE(result);
    EXPECT_EQ(request.queries.size(), 1);
    EXPECT_EQ(request.formula, "x");
    EXPECT_EQ(request.startTime, 0);
    EXPECT_EQ(request.endTime, 0);
    EXPECT_TRUE(request.aggregationInterval.empty());
}

TEST_F(DerivedQueryExecutorTest, ParseComplexFormula) {
    std::string json = R"json({
        "queries": {
            "success": "sum:http(requests){status:200}",
            "errors": "sum:http(requests){status:500}",
            "total": "sum:http(requests)"
        },
        "formula": "(success / (success + errors)) * 100",
        "startTime": 1000000000,
        "endTime": 2000000000
    })json";

    TestGlazeDerivedQueryRequest request;
    auto result = glz::read_json(request, json);

    EXPECT_FALSE(result);
    EXPECT_EQ(request.queries.size(), 3);
    EXPECT_EQ(request.formula, "(success / (success + errors)) * 100");
}

TEST_F(DerivedQueryExecutorTest, ParseInvalidJson) {
    std::string json = "{ invalid json }";

    TestGlazeDerivedQueryRequest request;
    auto result = glz::read_json(request, json);

    EXPECT_TRUE(result);  // Should have error
}

TEST_F(DerivedQueryExecutorTest, ParseEmptyQueries) {
    std::string json = R"json({
        "queries": {},
        "formula": "a + b"
    })json";

    TestGlazeDerivedQueryRequest request;
    auto result = glz::read_json(request, json);

    EXPECT_FALSE(result);
    EXPECT_TRUE(request.queries.empty());
}

// ==================== Response Formatting Tests ====================

TEST_F(DerivedQueryExecutorTest, FormatSuccessResponse) {
    DerivedQueryResult result;
    result.timestamps = {1000, 2000, 3000};
    result.values = {10.0, 20.0, 30.0};
    result.formula = "a + b";
    result.stats.pointCount = 3;
    result.stats.executionTimeMs = 15.5;
    result.stats.subQueriesExecuted = 2;
    result.stats.pointsDroppedDueToAlignment = 5;

    // Create executor with nullptr (we won't call execute)
    DerivedQueryExecutor executor(nullptr, nullptr);
    std::string json = executor.formatResponse(result);

    // Parse the JSON to verify structure
    TestGlazeDerivedQueryResponse response;
    auto parseResult = glz::read_json(response, json);

    EXPECT_FALSE(parseResult);
    EXPECT_EQ(response.status, "success");
    EXPECT_EQ(response.timestamps.size(), 3);
    EXPECT_EQ(response.timestamps[0], 1000);
    EXPECT_EQ(response.values.size(), 3);
    EXPECT_DOUBLE_EQ(response.values[1], 20.0);
    EXPECT_EQ(response.formula, "a + b");
    EXPECT_EQ(response.statistics.pointCount, 3);
    EXPECT_DOUBLE_EQ(response.statistics.executionTimeMs, 15.5);
    EXPECT_EQ(response.statistics.subQueriesExecuted, 2);
    EXPECT_EQ(response.statistics.pointsDroppedDueToAlignment, 5);
}

TEST_F(DerivedQueryExecutorTest, FormatEmptyResponse) {
    DerivedQueryResult result;
    result.formula = "empty_query";

    DerivedQueryExecutor executor(nullptr, nullptr);
    std::string json = executor.formatResponse(result);

    TestGlazeDerivedQueryResponse response;
    auto parseResult = glz::read_json(response, json);

    EXPECT_FALSE(parseResult);
    EXPECT_EQ(response.status, "success");
    EXPECT_TRUE(response.timestamps.empty());
    EXPECT_TRUE(response.values.empty());
}

TEST_F(DerivedQueryExecutorTest, FormatLargeResponse) {
    DerivedQueryResult result;
    result.formula = "large_test";

    // Generate 10000 points
    for (uint64_t i = 0; i < 10000; i++) {
        result.timestamps.push_back(i * 1000);
        result.values.push_back(static_cast<double>(i) * 1.5);
    }
    result.stats.pointCount = 10000;

    DerivedQueryExecutor executor(nullptr, nullptr);
    std::string json = executor.formatResponse(result);

    TestGlazeDerivedQueryResponse response;
    auto parseResult = glz::read_json(response, json);

    EXPECT_FALSE(parseResult);
    EXPECT_EQ(response.timestamps.size(), 10000);
    EXPECT_EQ(response.values.size(), 10000);
}

// ==================== Error Response Tests ====================

TEST_F(DerivedQueryExecutorTest, CreateErrorResponse) {
    std::string json = DerivedQueryExecutor::createErrorResponse("INVALID_FORMULA", "Division by zero");

    TestGlazeDerivedQueryResponse response;
    auto parseResult = glz::read_json(response, json);

    EXPECT_FALSE(parseResult);
    EXPECT_EQ(response.status, "error");
    EXPECT_EQ(response.error.code, "INVALID_FORMULA");
    EXPECT_EQ(response.error.message, "Division by zero");
}

TEST_F(DerivedQueryExecutorTest, CreateQueryParseErrorResponse) {
    std::string json = DerivedQueryExecutor::createErrorResponse(
        "QUERY_PARSE_ERROR",
        "Error parsing query 'a': Invalid measurement name");

    TestGlazeDerivedQueryResponse response;
    auto parseResult = glz::read_json(response, json);

    EXPECT_FALSE(parseResult);
    EXPECT_EQ(response.status, "error");
    EXPECT_EQ(response.error.code, "QUERY_PARSE_ERROR");
    EXPECT_TRUE(response.error.message.find("query 'a'") != std::string::npos);
}

TEST_F(DerivedQueryExecutorTest, CreateTimeoutErrorResponse) {
    std::string json = DerivedQueryExecutor::createErrorResponse(
        "TIMEOUT",
        "Query execution exceeded 30000ms timeout");

    TestGlazeDerivedQueryResponse response;
    auto parseResult = glz::read_json(response, json);

    EXPECT_FALSE(parseResult);
    EXPECT_EQ(response.status, "error");
    EXPECT_EQ(response.error.code, "TIMEOUT");
}

// ==================== Configuration Tests ====================

TEST_F(DerivedQueryExecutorTest, DefaultConfig) {
    DerivedQueryConfig config;

    EXPECT_EQ(config.alignmentStrategy, AlignmentStrategy::INNER);
    EXPECT_EQ(config.interpolationMethod, InterpolationMethod::LINEAR);
    EXPECT_EQ(config.maxSubQueries, 10);
    EXPECT_EQ(config.maxTotalPoints, 10000000);
    EXPECT_EQ(config.timeoutMs, 30000);
}

TEST_F(DerivedQueryExecutorTest, CustomConfig) {
    DerivedQueryConfig config;
    config.alignmentStrategy = AlignmentStrategy::OUTER;
    config.interpolationMethod = InterpolationMethod::ZERO;
    config.maxSubQueries = 20;
    config.maxTotalPoints = 5000000;
    config.timeoutMs = 60000;

    // Verify construction with custom config does not throw
    EXPECT_NO_THROW({
        DerivedQueryExecutor executor(nullptr, nullptr, config);
    });
}

// ==================== Request Validation Tests ====================

TEST_F(DerivedQueryExecutorTest, ValidateEmptyFormula) {
    DerivedQueryRequest request;
    request.formula = "";
    request.queries["a"] = QueryRequest();

    EXPECT_THROW(request.validate(), DerivedQueryException);
}

TEST_F(DerivedQueryExecutorTest, ValidateNoQueries) {
    DerivedQueryRequest request;
    request.formula = "a + b";
    // No queries defined

    EXPECT_THROW(request.validate(), DerivedQueryException);
}

TEST_F(DerivedQueryExecutorTest, ValidateValidRequest) {
    DerivedQueryRequest request;
    request.formula = "a + b";
    request.queries["a"] = QueryRequest();
    request.queries["b"] = QueryRequest();
    request.startTime = 1000;
    request.endTime = 2000;

    EXPECT_NO_THROW(request.validate());
}

TEST_F(DerivedQueryExecutorTest, ValidateInvalidTimeRange) {
    DerivedQueryRequest request;
    request.formula = "a";
    request.queries["a"] = QueryRequest();
    request.startTime = 2000;
    request.endTime = 1000;  // End before start

    EXPECT_THROW(request.validate(), DerivedQueryException);
}

// ==================== Query Reference Detection Tests ====================

TEST_F(DerivedQueryExecutorTest, GetReferencedQueriesSimple) {
    DerivedQueryRequest request;
    request.formula = "a";
    request.queries["a"] = QueryRequest();
    request.queries["b"] = QueryRequest();

    auto refs = request.getReferencedQueries();

    EXPECT_EQ(refs.size(), 1);
    EXPECT_TRUE(refs.count("a"));
    EXPECT_FALSE(refs.count("b"));
}

TEST_F(DerivedQueryExecutorTest, GetReferencedQueriesComplex) {
    DerivedQueryRequest request;
    request.formula = "(a + b) / (c - a)";
    request.queries["a"] = QueryRequest();
    request.queries["b"] = QueryRequest();
    request.queries["c"] = QueryRequest();
    request.queries["unused"] = QueryRequest();

    auto refs = request.getReferencedQueries();

    EXPECT_EQ(refs.size(), 3);
    EXPECT_TRUE(refs.count("a"));
    EXPECT_TRUE(refs.count("b"));
    EXPECT_TRUE(refs.count("c"));
    EXPECT_FALSE(refs.count("unused"));
}

TEST_F(DerivedQueryExecutorTest, GetReferencedQueriesWithFunctions) {
    DerivedQueryRequest request;
    request.formula = "abs(a) + max(b, c)";
    request.queries["a"] = QueryRequest();
    request.queries["b"] = QueryRequest();
    request.queries["c"] = QueryRequest();

    auto refs = request.getReferencedQueries();

    EXPECT_EQ(refs.size(), 3);
    EXPECT_TRUE(refs.count("a"));
    EXPECT_TRUE(refs.count("b"));
    EXPECT_TRUE(refs.count("c"));
}

// ==================== Builder Pattern Tests ====================

TEST_F(DerivedQueryExecutorTest, BuilderCreatesValidRequest) {
    auto request = DerivedQueryBuilder()
        .addQuery("cpu", "avg:cpu(usage){host:server1}")
        .addQuery("mem", "avg:memory(used){host:server1}")
        .setFormula("cpu / mem * 100")
        .setTimeRange(1000, 2000)
        .setAggregationInterval(60000)
        .build();

    EXPECT_EQ(request.formula, "cpu / mem * 100");
    EXPECT_EQ(request.startTime, 1000);
    EXPECT_EQ(request.endTime, 2000);
    EXPECT_EQ(request.aggregationInterval, 60000);
    EXPECT_EQ(request.queries.size(), 2);
}

// ==================== Glaze Response Serialization Tests ====================

TEST_F(DerivedQueryExecutorTest, StatisticsSerializationRoundtrip) {
    TestGlazeDerivedQueryResponse::Statistics stats;
    stats.pointCount = 42;
    stats.executionTimeMs = 123.456;
    stats.subQueriesExecuted = 3;
    stats.pointsDroppedDueToAlignment = 7;

    // Create full response with stats
    TestGlazeDerivedQueryResponse response;
    response.status = "success";
    response.statistics = stats;

    auto json = glz::write_json(response);
    ASSERT_TRUE(json.has_value());

    TestGlazeDerivedQueryResponse parsed;
    auto result = glz::read_json(parsed, json.value());

    EXPECT_FALSE(result);
    EXPECT_EQ(parsed.statistics.pointCount, 42);
    EXPECT_DOUBLE_EQ(parsed.statistics.executionTimeMs, 123.456);
    EXPECT_EQ(parsed.statistics.subQueriesExecuted, 3);
    EXPECT_EQ(parsed.statistics.pointsDroppedDueToAlignment, 7);
}

TEST_F(DerivedQueryExecutorTest, ErrorSerializationRoundtrip) {
    TestGlazeDerivedQueryResponse::Error error;
    error.code = "TEST_ERROR";
    error.message = "This is a test error message";

    TestGlazeDerivedQueryResponse response;
    response.status = "error";
    response.error = error;

    auto json = glz::write_json(response);
    ASSERT_TRUE(json.has_value());

    TestGlazeDerivedQueryResponse parsed;
    auto result = glz::read_json(parsed, json.value());

    EXPECT_FALSE(result);
    EXPECT_EQ(parsed.status, "error");
    EXPECT_EQ(parsed.error.code, "TEST_ERROR");
    EXPECT_EQ(parsed.error.message, "This is a test error message");
}

// ==================== Edge Cases ====================

TEST_F(DerivedQueryExecutorTest, SpecialCharactersInFormula) {
    std::string json = R"json({
        "queries": {
            "metric_1": "avg:system.cpu(usage)"
        },
        "formula": "metric_1 * 2.5"
    })json";

    TestGlazeDerivedQueryRequest request;
    auto result = glz::read_json(request, json);

    EXPECT_FALSE(result);
    EXPECT_EQ(request.queries["metric_1"], "avg:system.cpu(usage)");
}

TEST_F(DerivedQueryExecutorTest, UnicodeInQueryNames) {
    // While not recommended, test that system handles unusual query names
    DerivedQueryRequest request;
    request.formula = "query_with_numbers_123";
    request.queries["query_with_numbers_123"] = QueryRequest();

    EXPECT_NO_THROW(request.validate());
}

TEST_F(DerivedQueryExecutorTest, VeryLongFormula) {
    std::string longFormula = "a";
    for (int i = 0; i < 100; i++) {
        longFormula += " + a";
    }

    DerivedQueryRequest request;
    request.formula = longFormula;
    request.queries["a"] = QueryRequest();
    request.startTime = 1000;
    request.endTime = 2000;

    EXPECT_NO_THROW(request.validate());
}
