#include <gtest/gtest.h>
#include <glaze/glaze.hpp>

#include "../../../lib/http/http_derived_query_handler.hpp"
#include "../../../lib/query/derived_query.hpp"
#include "../../../lib/query/derived_query_executor.hpp"
#include "../../../lib/query/expression_parser.hpp"

using namespace timestar;

// =============================================================================
// Glaze structures for parsing test responses
// =============================================================================

// Re-declare Glaze meta for request parsing (must match derived_query_executor.cpp)
struct TestGlazeDerivedQueryRequest {
    std::map<std::string, std::string> queries;
    std::string formula;
    uint64_t startTime = 0;
    uint64_t endTime = 0;
    std::string aggregationInterval;
};

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

// Structure for parsing derived query JSON responses
struct TestDerivedResponseStatistics {
    size_t point_count = 0;
    double execution_time_ms = 0.0;
    size_t sub_queries_executed = 0;
    size_t points_dropped_due_to_alignment = 0;
};

struct TestDerivedResponseError {
    std::string code;
    std::string message;
};

struct TestDerivedResponse {
    std::string status;
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    std::string formula;
    TestDerivedResponseStatistics statistics;
    TestDerivedResponseError error;
};

template <>
struct glz::meta<TestDerivedResponseStatistics> {
    using T = TestDerivedResponseStatistics;
    static constexpr auto value = object(
        "point_count", &T::point_count,
        "execution_time_ms", &T::execution_time_ms,
        "sub_queries_executed", &T::sub_queries_executed,
        "points_dropped_due_to_alignment", &T::points_dropped_due_to_alignment
    );
};

template <>
struct glz::meta<TestDerivedResponseError> {
    using T = TestDerivedResponseError;
    static constexpr auto value = object(
        "code", &T::code,
        "message", &T::message
    );
};

template <>
struct glz::meta<TestDerivedResponse> {
    using T = TestDerivedResponse;
    static constexpr auto value = object(
        "status", &T::status,
        "timestamps", &T::timestamps,
        "values", &T::values,
        "formula", &T::formula,
        "statistics", &T::statistics,
        "error", &T::error
    );
};

// =============================================================================
// Test fixture
// =============================================================================

class HttpDerivedQueryHandlerTest : public ::testing::Test {
protected:
    // Executor with nullptr engine/index for testing synchronous (non-async) methods
    DerivedQueryExecutor executor{nullptr, nullptr};
};

// =============================================================================
// MAX_DERIVED_QUERY_BODY_SIZE Constant Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, MaxBodySizeIs1MB) {
    EXPECT_EQ(HttpDerivedQueryHandler::MAX_DERIVED_QUERY_BODY_SIZE, 1 * 1024 * 1024);
}

// =============================================================================
// DerivedQueryExecutor::createErrorResponse Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, CreateErrorResponseBasic) {
    std::string json = DerivedQueryExecutor::createErrorResponse(
        "QUERY_ERROR", "Invalid formula syntax");

    TestDerivedResponse parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse error response JSON: " << glz::format_error(ec);

    EXPECT_EQ(parsed.status, "error");
    EXPECT_EQ(parsed.error.code, "QUERY_ERROR");
    EXPECT_EQ(parsed.error.message, "Invalid formula syntax");
}

TEST_F(HttpDerivedQueryHandlerTest, CreateErrorResponseEmptyBody) {
    std::string json = DerivedQueryExecutor::createErrorResponse(
        "EMPTY_REQUEST", "Request body is required");

    TestDerivedResponse parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse error response JSON: " << glz::format_error(ec);

    EXPECT_EQ(parsed.status, "error");
    EXPECT_EQ(parsed.error.code, "EMPTY_REQUEST");
    EXPECT_EQ(parsed.error.message, "Request body is required");
}

TEST_F(HttpDerivedQueryHandlerTest, CreateErrorResponseBodyTooLarge) {
    std::string json = DerivedQueryExecutor::createErrorResponse(
        "BODY_TOO_LARGE", "Request body too large (max 1MB)");

    TestDerivedResponse parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse error response JSON: " << glz::format_error(ec);

    EXPECT_EQ(parsed.status, "error");
    EXPECT_EQ(parsed.error.code, "BODY_TOO_LARGE");
    EXPECT_EQ(parsed.error.message, "Request body too large (max 1MB)");
}

TEST_F(HttpDerivedQueryHandlerTest, CreateErrorResponseUnsupportedMediaType) {
    std::string json = DerivedQueryExecutor::createErrorResponse(
        "UNSUPPORTED_MEDIA_TYPE", "Content-Type must be application/json");

    TestDerivedResponse parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse error response JSON: " << glz::format_error(ec);

    EXPECT_EQ(parsed.status, "error");
    EXPECT_EQ(parsed.error.code, "UNSUPPORTED_MEDIA_TYPE");
    EXPECT_EQ(parsed.error.message, "Content-Type must be application/json");
}

TEST_F(HttpDerivedQueryHandlerTest, CreateErrorResponseInternalError) {
    std::string json = DerivedQueryExecutor::createErrorResponse(
        "INTERNAL_ERROR", "Unexpected error during execution");

    TestDerivedResponse parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse error response JSON: " << glz::format_error(ec);

    EXPECT_EQ(parsed.status, "error");
    EXPECT_EQ(parsed.error.code, "INTERNAL_ERROR");
    EXPECT_EQ(parsed.error.message, "Unexpected error during execution");
}

TEST_F(HttpDerivedQueryHandlerTest, CreateErrorResponseSpecialCharacters) {
    std::string json = DerivedQueryExecutor::createErrorResponse(
        "QUERY_ERROR", "Unexpected token '<' at position 5");

    TestDerivedResponse parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse error response with special chars: "
                     << glz::format_error(ec);

    EXPECT_EQ(parsed.status, "error");
    EXPECT_NE(parsed.error.message.find("<"), std::string::npos);
}

// =============================================================================
// DerivedQueryExecutor::formatResponse Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, FormatResponseEmpty) {
    DerivedQueryResult result;
    result.formula = "a + b";
    result.stats.pointCount = 0;
    result.stats.executionTimeMs = 1.5;
    result.stats.subQueriesExecuted = 2;
    result.stats.pointsDroppedDueToAlignment = 0;

    std::string json = executor.formatResponse(result);

    TestDerivedResponse parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse response JSON: " << glz::format_error(ec);

    EXPECT_EQ(parsed.status, "success");
    EXPECT_EQ(parsed.formula, "a + b");
    EXPECT_TRUE(parsed.timestamps.empty());
    EXPECT_TRUE(parsed.values.empty());
    EXPECT_EQ(parsed.statistics.point_count, 0u);
    EXPECT_DOUBLE_EQ(parsed.statistics.execution_time_ms, 1.5);
    EXPECT_EQ(parsed.statistics.sub_queries_executed, 2u);
    EXPECT_EQ(parsed.statistics.points_dropped_due_to_alignment, 0u);
}

TEST_F(HttpDerivedQueryHandlerTest, FormatResponseWithData) {
    DerivedQueryResult result;
    result.formula = "(a + b) / 2";
    result.timestamps = {1000000000, 2000000000, 3000000000};
    result.values = {10.5, 20.0, 30.5};
    result.stats.pointCount = 3;
    result.stats.executionTimeMs = 12.5;
    result.stats.subQueriesExecuted = 2;
    result.stats.pointsDroppedDueToAlignment = 1;

    std::string json = executor.formatResponse(result);

    TestDerivedResponse parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse response JSON: " << glz::format_error(ec);

    EXPECT_EQ(parsed.status, "success");
    EXPECT_EQ(parsed.formula, "(a + b) / 2");

    ASSERT_EQ(parsed.timestamps.size(), 3u);
    EXPECT_EQ(parsed.timestamps[0], 1000000000u);
    EXPECT_EQ(parsed.timestamps[1], 2000000000u);
    EXPECT_EQ(parsed.timestamps[2], 3000000000u);

    ASSERT_EQ(parsed.values.size(), 3u);
    EXPECT_DOUBLE_EQ(parsed.values[0], 10.5);
    EXPECT_DOUBLE_EQ(parsed.values[1], 20.0);
    EXPECT_DOUBLE_EQ(parsed.values[2], 30.5);

    EXPECT_EQ(parsed.statistics.point_count, 3u);
    EXPECT_DOUBLE_EQ(parsed.statistics.execution_time_ms, 12.5);
    EXPECT_EQ(parsed.statistics.sub_queries_executed, 2u);
    EXPECT_EQ(parsed.statistics.points_dropped_due_to_alignment, 1u);
}

TEST_F(HttpDerivedQueryHandlerTest, FormatResponseSinglePoint) {
    DerivedQueryResult result;
    result.formula = "a * 100";
    result.timestamps = {5000000000};
    result.values = {99.9};
    result.stats.pointCount = 1;
    result.stats.executionTimeMs = 0.5;
    result.stats.subQueriesExecuted = 1;

    std::string json = executor.formatResponse(result);

    TestDerivedResponse parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse response JSON: " << glz::format_error(ec);

    EXPECT_EQ(parsed.status, "success");
    ASSERT_EQ(parsed.timestamps.size(), 1u);
    EXPECT_EQ(parsed.timestamps[0], 5000000000u);
    ASSERT_EQ(parsed.values.size(), 1u);
    EXPECT_DOUBLE_EQ(parsed.values[0], 99.9);
}

TEST_F(HttpDerivedQueryHandlerTest, FormatResponseLargeDataset) {
    DerivedQueryResult result;
    result.formula = "a - b";

    // Generate 1000 points
    for (size_t i = 0; i < 1000; ++i) {
        result.timestamps.push_back(1000000000 + i * 1000000);
        result.values.push_back(static_cast<double>(i) * 0.1);
    }
    result.stats.pointCount = 1000;
    result.stats.executionTimeMs = 50.0;
    result.stats.subQueriesExecuted = 2;

    std::string json = executor.formatResponse(result);

    TestDerivedResponse parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse large response JSON: " << glz::format_error(ec);

    EXPECT_EQ(parsed.status, "success");
    EXPECT_EQ(parsed.timestamps.size(), 1000u);
    EXPECT_EQ(parsed.values.size(), 1000u);
    EXPECT_EQ(parsed.statistics.point_count, 1000u);
}

// =============================================================================
// DerivedQueryExecutor::formatResponseVariant Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, FormatResponseVariantDerivedQuery) {
    DerivedQueryResult result;
    result.formula = "a + b";
    result.timestamps = {1000, 2000};
    result.values = {5.0, 10.0};
    result.stats.pointCount = 2;

    DerivedQueryResultVariant variant{result};
    std::string json = executor.formatResponseVariant(variant);

    TestDerivedResponse parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse variant response JSON: " << glz::format_error(ec);

    EXPECT_EQ(parsed.status, "success");
    EXPECT_EQ(parsed.formula, "a + b");
    EXPECT_EQ(parsed.timestamps.size(), 2u);
}

TEST_F(HttpDerivedQueryHandlerTest, FormatResponseVariantAnomalyResult) {
    anomaly::AnomalyQueryResult anomalyResult;
    anomalyResult.success = true;
    anomalyResult.times = {1000, 2000, 3000};
    anomalyResult.statistics.algorithm = "basic";
    anomalyResult.statistics.bounds = 2.0;
    anomalyResult.statistics.anomalyCount = 1;
    anomalyResult.statistics.totalPoints = 3;
    anomalyResult.statistics.executionTimeMs = 5.0;

    DerivedQueryResultVariant variant{anomalyResult};
    std::string json = executor.formatResponseVariant(variant);

    // Parse as generic JSON to verify structure
    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse anomaly variant JSON: " << glz::format_error(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "success");

    auto& times = obj["times"].get<glz::generic::array_t>();
    EXPECT_EQ(times.size(), 3u);
}

TEST_F(HttpDerivedQueryHandlerTest, FormatResponseVariantForecastResult) {
    forecast::ForecastQueryResult forecastResult;
    forecastResult.success = true;
    forecastResult.times = {1000, 2000, 3000, 4000};
    forecastResult.forecastStartIndex = 2;
    forecastResult.statistics.algorithm = "linear";
    forecastResult.statistics.historicalPoints = 2;
    forecastResult.statistics.forecastPoints = 2;
    forecastResult.statistics.executionTimeMs = 3.0;

    DerivedQueryResultVariant variant{forecastResult};
    std::string json = executor.formatResponseVariant(variant);

    // Parse as generic JSON to verify structure
    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse forecast variant JSON: " << glz::format_error(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "success");

    auto& times = obj["times"].get<glz::generic::array_t>();
    EXPECT_EQ(times.size(), 4u);

    EXPECT_EQ(static_cast<size_t>(obj["forecast_start_index"].get<double>()), 2u);
}

// =============================================================================
// DerivedQueryExecutor::formatAnomalyResponse Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, FormatAnomalyResponseSuccess) {
    anomaly::AnomalyQueryResult result;
    result.success = true;
    result.times = {1000000000, 2000000000, 3000000000};
    result.statistics.algorithm = "robust";
    result.statistics.bounds = 3.0;
    result.statistics.seasonality = "daily";
    result.statistics.anomalyCount = 2;
    result.statistics.totalPoints = 3;
    result.statistics.executionTimeMs = 8.0;

    // Add a series piece
    anomaly::AnomalySeriesPiece piece;
    piece.piece = "raw";
    piece.groupTags = {"host=server01"};
    piece.values = {10.0, 20.0, 30.0};
    result.series.push_back(piece);

    std::string json = executor.formatAnomalyResponse(result);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse anomaly response JSON: " << glz::format_error(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "success");

    auto& times = obj["times"].get<glz::generic::array_t>();
    EXPECT_EQ(times.size(), 3u);

    auto& series = obj["series"].get<glz::generic::array_t>();
    EXPECT_EQ(series.size(), 1u);

    auto& stats = obj["statistics"].get<glz::generic::object_t>();
    EXPECT_EQ(stats["algorithm"].get<std::string>(), "robust");
    EXPECT_DOUBLE_EQ(stats["bounds"].get<double>(), 3.0);
    EXPECT_EQ(stats["seasonality"].get<std::string>(), "daily");
    EXPECT_EQ(static_cast<size_t>(stats["anomaly_count"].get<double>()), 2u);
    EXPECT_EQ(static_cast<size_t>(stats["total_points"].get<double>()), 3u);
}

TEST_F(HttpDerivedQueryHandlerTest, FormatAnomalyResponseError) {
    anomaly::AnomalyQueryResult result;
    result.success = false;
    result.errorMessage = "Insufficient data for anomaly detection";

    std::string json = executor.formatAnomalyResponse(result);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse anomaly error response JSON: " << glz::format_error(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "error");

    auto& errorObj = obj["error"].get<glz::generic::object_t>();
    EXPECT_EQ(errorObj["message"].get<std::string>(), "Insufficient data for anomaly detection");
}

// =============================================================================
// DerivedQueryExecutor::formatForecastResponse Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, FormatForecastResponseSuccess) {
    forecast::ForecastQueryResult result;
    result.success = true;
    result.times = {1000, 2000, 3000, 4000, 5000};
    result.forecastStartIndex = 3;
    result.statistics.algorithm = "linear";
    result.statistics.deviations = 2.0;
    result.statistics.slope = 1.5;
    result.statistics.intercept = 10.0;
    result.statistics.rSquared = 0.95;
    result.statistics.residualStdDev = 0.5;
    result.statistics.historicalPoints = 3;
    result.statistics.forecastPoints = 2;
    result.statistics.seriesCount = 1;
    result.statistics.executionTimeMs = 4.0;

    // Add a series piece
    forecast::ForecastSeriesPiece piece;
    piece.piece = "forecast";
    piece.groupTags = {"region=us-west"};
    piece.values = {10.0, 11.5, 13.0, 14.5, 16.0};
    result.series.push_back(piece);

    std::string json = executor.formatForecastResponse(result);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse forecast response JSON: " << glz::format_error(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "success");

    auto& times = obj["times"].get<glz::generic::array_t>();
    EXPECT_EQ(times.size(), 5u);

    EXPECT_EQ(static_cast<size_t>(obj["forecast_start_index"].get<double>()), 3u);

    auto& stats = obj["statistics"].get<glz::generic::object_t>();
    EXPECT_EQ(stats["algorithm"].get<std::string>(), "linear");
    EXPECT_DOUBLE_EQ(stats["deviations"].get<double>(), 2.0);
    EXPECT_DOUBLE_EQ(stats["slope"].get<double>(), 1.5);
    EXPECT_DOUBLE_EQ(stats["intercept"].get<double>(), 10.0);
    EXPECT_DOUBLE_EQ(stats["r_squared"].get<double>(), 0.95);
    EXPECT_EQ(static_cast<size_t>(stats["historical_points"].get<double>()), 3u);
    EXPECT_EQ(static_cast<size_t>(stats["forecast_points"].get<double>()), 2u);
}

TEST_F(HttpDerivedQueryHandlerTest, FormatForecastResponseError) {
    forecast::ForecastQueryResult result;
    result.success = false;
    result.errorMessage = "Not enough data points for forecast";

    std::string json = executor.formatForecastResponse(result);

    glz::generic parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec) << "Failed to parse forecast error response JSON: " << glz::format_error(ec);

    auto& obj = parsed.get<glz::generic::object_t>();
    EXPECT_EQ(obj["status"].get<std::string>(), "error");

    auto& errorObj = obj["error"].get<glz::generic::object_t>();
    EXPECT_EQ(errorObj["message"].get<std::string>(), "Not enough data points for forecast");
}

// =============================================================================
// DerivedQueryExecutor::isAnomalyFormula Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, IsAnomalyFormulaTrue) {
    EXPECT_TRUE(DerivedQueryExecutor::isAnomalyFormula("anomalies(a, 'basic', 2)"));
    EXPECT_TRUE(DerivedQueryExecutor::isAnomalyFormula("anomalies(cpu, 'robust', 3, 'daily')"));
}

TEST_F(HttpDerivedQueryHandlerTest, IsAnomalyFormulaWithLeadingWhitespace) {
    EXPECT_TRUE(DerivedQueryExecutor::isAnomalyFormula("  anomalies(a, 'basic', 2)"));
    EXPECT_TRUE(DerivedQueryExecutor::isAnomalyFormula("\t anomalies(a, 'agile', 2)"));
    EXPECT_TRUE(DerivedQueryExecutor::isAnomalyFormula("\n anomalies(a, 'basic', 2)"));
}

TEST_F(HttpDerivedQueryHandlerTest, IsAnomalyFormulaFalse) {
    EXPECT_FALSE(DerivedQueryExecutor::isAnomalyFormula("a + b"));
    EXPECT_FALSE(DerivedQueryExecutor::isAnomalyFormula("(a + b) / 2"));
    EXPECT_FALSE(DerivedQueryExecutor::isAnomalyFormula("abs(a)"));
    EXPECT_FALSE(DerivedQueryExecutor::isAnomalyFormula("forecast(a, 'linear', 2)"));
    EXPECT_FALSE(DerivedQueryExecutor::isAnomalyFormula(""));
}

// =============================================================================
// DerivedQueryExecutor::isForecastFormula Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, IsForecastFormulaTrue) {
    EXPECT_TRUE(DerivedQueryExecutor::isForecastFormula("forecast(a, 'linear', 2)"));
    EXPECT_TRUE(DerivedQueryExecutor::isForecastFormula("forecast(cpu, 'seasonal', 3, 'daily')"));
}

TEST_F(HttpDerivedQueryHandlerTest, IsForecastFormulaWithLeadingWhitespace) {
    EXPECT_TRUE(DerivedQueryExecutor::isForecastFormula("  forecast(a, 'linear', 2)"));
    EXPECT_TRUE(DerivedQueryExecutor::isForecastFormula("\t forecast(a, 'seasonal', 2)"));
}

TEST_F(HttpDerivedQueryHandlerTest, IsForecastFormulaFalse) {
    EXPECT_FALSE(DerivedQueryExecutor::isForecastFormula("a + b"));
    EXPECT_FALSE(DerivedQueryExecutor::isForecastFormula("anomalies(a, 'basic', 2)"));
    EXPECT_FALSE(DerivedQueryExecutor::isForecastFormula("abs(a)"));
    EXPECT_FALSE(DerivedQueryExecutor::isForecastFormula(""));
}

// =============================================================================
// DerivedQueryRequest Validation Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, ValidateRequestEmptyQueries) {
    DerivedQueryRequest request;
    request.formula = "a + b";

    EXPECT_THROW(request.validate(), DerivedQueryException);
}

TEST_F(HttpDerivedQueryHandlerTest, ValidateRequestEmptyFormula) {
    DerivedQueryRequest request;
    QueryRequest subQuery;
    subQuery.measurement = "temperature";
    subQuery.startTime = 1000;
    subQuery.endTime = 2000;
    request.queries["a"] = subQuery;
    request.formula = "";

    EXPECT_THROW(request.validate(), DerivedQueryException);
}

TEST_F(HttpDerivedQueryHandlerTest, ValidateRequestInvalidTimeRange) {
    DerivedQueryRequest request;
    QueryRequest subQuery;
    subQuery.measurement = "temperature";
    subQuery.startTime = 1000;
    subQuery.endTime = 2000;
    request.queries["a"] = subQuery;
    request.formula = "a";
    request.startTime = 5000;
    request.endTime = 1000;  // endTime < startTime

    EXPECT_THROW(request.validate(), DerivedQueryException);
}

TEST_F(HttpDerivedQueryHandlerTest, ValidateRequestPartialTimeRange) {
    DerivedQueryRequest request;
    QueryRequest subQuery;
    subQuery.measurement = "temperature";
    subQuery.startTime = 1000;
    subQuery.endTime = 2000;
    request.queries["a"] = subQuery;
    request.formula = "a";
    request.startTime = 1000;
    request.endTime = 0;  // Only startTime set

    EXPECT_THROW(request.validate(), DerivedQueryException);
}

TEST_F(HttpDerivedQueryHandlerTest, ValidateRequestUndefinedQueryRef) {
    DerivedQueryRequest request;
    QueryRequest subQuery;
    subQuery.measurement = "temperature";
    subQuery.startTime = 1000;
    subQuery.endTime = 2000;
    request.queries["a"] = subQuery;
    request.formula = "a + b";  // "b" is not defined

    EXPECT_THROW(request.validate(), DerivedQueryException);
}

TEST_F(HttpDerivedQueryHandlerTest, ValidateRequestInvalidFormulaSyntax) {
    DerivedQueryRequest request;
    QueryRequest subQuery;
    subQuery.measurement = "temperature";
    subQuery.startTime = 1000;
    subQuery.endTime = 2000;
    request.queries["a"] = subQuery;
    request.formula = "a + + b";  // Invalid syntax

    EXPECT_THROW(request.validate(), DerivedQueryException);
}

TEST_F(HttpDerivedQueryHandlerTest, ValidateRequestValidSimple) {
    DerivedQueryRequest request;
    QueryRequest subQuery;
    subQuery.measurement = "temperature";
    subQuery.startTime = 1000;
    subQuery.endTime = 2000;
    request.queries["a"] = subQuery;
    request.formula = "a";

    EXPECT_NO_THROW(request.validate());
}

TEST_F(HttpDerivedQueryHandlerTest, ValidateRequestValidWithFormula) {
    DerivedQueryRequest request;

    QueryRequest subQueryA;
    subQueryA.measurement = "cpu";
    subQueryA.startTime = 1000;
    subQueryA.endTime = 2000;
    request.queries["a"] = subQueryA;

    QueryRequest subQueryB;
    subQueryB.measurement = "memory";
    subQueryB.startTime = 1000;
    subQueryB.endTime = 2000;
    request.queries["b"] = subQueryB;

    request.formula = "(a + b) / 2";

    EXPECT_NO_THROW(request.validate());
}

TEST_F(HttpDerivedQueryHandlerTest, ValidateRequestValidWithTimeRange) {
    DerivedQueryRequest request;
    QueryRequest subQuery;
    subQuery.measurement = "temperature";
    subQuery.startTime = 1000;
    subQuery.endTime = 2000;
    request.queries["a"] = subQuery;
    request.formula = "a * 100";
    request.startTime = 1000;
    request.endTime = 2000;

    EXPECT_NO_THROW(request.validate());
}

// =============================================================================
// DerivedQueryRequest Helper Method Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, GetReferencedQueries) {
    DerivedQueryRequest request;
    QueryRequest subQueryA;
    subQueryA.measurement = "cpu";
    subQueryA.startTime = 1000;
    subQueryA.endTime = 2000;
    request.queries["a"] = subQueryA;

    QueryRequest subQueryB;
    subQueryB.measurement = "memory";
    subQueryB.startTime = 1000;
    subQueryB.endTime = 2000;
    request.queries["b"] = subQueryB;

    request.formula = "a + b";

    auto refs = request.getReferencedQueries();
    EXPECT_EQ(refs.size(), 2u);
    EXPECT_GT(refs.count("a"), 0u);
    EXPECT_GT(refs.count("b"), 0u);
}

TEST_F(HttpDerivedQueryHandlerTest, ApplyGlobalTimeRange) {
    DerivedQueryRequest request;
    request.startTime = 5000;
    request.endTime = 10000;

    QueryRequest subQuery;
    subQuery.measurement = "temperature";
    subQuery.startTime = 0;  // Not set
    subQuery.endTime = 0;    // Not set
    request.queries["a"] = subQuery;

    request.applyGlobalTimeRange();

    EXPECT_EQ(request.queries["a"].startTime, 5000u);
    EXPECT_EQ(request.queries["a"].endTime, 10000u);
}

TEST_F(HttpDerivedQueryHandlerTest, ApplyGlobalTimeRangePreservesExisting) {
    DerivedQueryRequest request;
    request.startTime = 5000;
    request.endTime = 10000;

    QueryRequest subQuery;
    subQuery.measurement = "temperature";
    subQuery.startTime = 3000;  // Already set
    subQuery.endTime = 8000;    // Already set
    request.queries["a"] = subQuery;

    request.applyGlobalTimeRange();

    // Should preserve the per-query time range
    EXPECT_EQ(request.queries["a"].startTime, 3000u);
    EXPECT_EQ(request.queries["a"].endTime, 8000u);
}

// =============================================================================
// DerivedQueryBuilder Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, BuilderBasicQuery) {
    DerivedQueryBuilder builder;

    QueryRequest subQuery;
    subQuery.measurement = "cpu";
    subQuery.startTime = 1000;
    subQuery.endTime = 2000;

    auto request = builder
        .addQuery("a", subQuery)
        .setFormula("a")
        .setTimeRange(1000, 2000)
        .build();

    EXPECT_EQ(request.queries.size(), 1u);
    EXPECT_EQ(request.formula, "a");
    EXPECT_EQ(request.startTime, 1000u);
    EXPECT_EQ(request.endTime, 2000u);
}

TEST_F(HttpDerivedQueryHandlerTest, BuilderMultipleQueries) {
    DerivedQueryBuilder builder;

    QueryRequest subQueryA;
    subQueryA.measurement = "cpu";
    subQueryA.startTime = 1000;
    subQueryA.endTime = 2000;

    QueryRequest subQueryB;
    subQueryB.measurement = "memory";
    subQueryB.startTime = 1000;
    subQueryB.endTime = 2000;

    auto request = builder
        .addQuery("a", subQueryA)
        .addQuery("b", subQueryB)
        .setFormula("a + b")
        .setTimeRange(1000, 2000)
        .build();

    EXPECT_EQ(request.queries.size(), 2u);
    EXPECT_EQ(request.formula, "a + b");
}

TEST_F(HttpDerivedQueryHandlerTest, BuilderWithQueryString) {
    DerivedQueryBuilder builder;

    auto request = builder
        .addQuery("a", "avg:cpu(usage)")
        .setFormula("a")
        .setTimeRange(1000, 2000)
        .build();

    EXPECT_EQ(request.queries.size(), 1u);
    EXPECT_EQ(request.queries.at("a").measurement, "cpu");
    EXPECT_EQ(request.queries.at("a").fields.size(), 1u);
    EXPECT_EQ(request.queries.at("a").fields[0], "usage");
}

TEST_F(HttpDerivedQueryHandlerTest, BuilderWithAggregationInterval) {
    DerivedQueryBuilder builder;

    QueryRequest subQuery;
    subQuery.measurement = "cpu";
    subQuery.startTime = 1000;
    subQuery.endTime = 2000;

    auto request = builder
        .addQuery("a", subQuery)
        .setFormula("a")
        .setTimeRange(1000, 2000)
        .setAggregationInterval(300000000000ULL)  // 5 minutes
        .build();

    EXPECT_EQ(request.aggregationInterval, 300000000000ULL);
}

TEST_F(HttpDerivedQueryHandlerTest, BuilderValidationFailsOnBuild) {
    DerivedQueryBuilder builder;

    // No queries added, formula references "a"
    EXPECT_THROW(
        builder.setFormula("a").setTimeRange(1000, 2000).build(),
        DerivedQueryException);
}

// =============================================================================
// JSON Request Parsing Tests (using Glaze directly)
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, ParseValidJsonRequest) {
    std::string jsonStr = R"json({
        "queries": {
            "a": "avg:cpu(usage){host:server01}",
            "b": "avg:memory(used)"
        },
        "formula": "(a + b) / 2",
        "startTime": 1704067200000000000,
        "endTime": 1704153600000000000
    })json";

    TestGlazeDerivedQueryRequest parsed;
    auto ec = glz::read_json(parsed, jsonStr);
    ASSERT_FALSE(ec) << "Failed to parse derived query JSON: " << glz::format_error(ec);

    EXPECT_EQ(parsed.queries.size(), 2u);
    EXPECT_EQ(parsed.queries.at("a"), "avg:cpu(usage){host:server01}");
    EXPECT_EQ(parsed.queries.at("b"), "avg:memory(used)");
    EXPECT_EQ(parsed.formula, "(a + b) / 2");
    EXPECT_EQ(parsed.startTime, 1704067200000000000ULL);
    EXPECT_EQ(parsed.endTime, 1704153600000000000ULL);
}

TEST_F(HttpDerivedQueryHandlerTest, ParseJsonRequestWithInterval) {
    std::string jsonStr = R"json({
        "queries": {"a": "avg:cpu(usage)"},
        "formula": "a",
        "startTime": 1000000000,
        "endTime": 2000000000,
        "aggregationInterval": "5m"
    })json";

    TestGlazeDerivedQueryRequest parsed;
    auto ec = glz::read_json(parsed, jsonStr);
    ASSERT_FALSE(ec) << "Failed to parse JSON with interval: " << glz::format_error(ec);

    EXPECT_EQ(parsed.aggregationInterval, "5m");
}

TEST_F(HttpDerivedQueryHandlerTest, ParseJsonRequestMinimal) {
    std::string jsonStr = R"json({
        "queries": {"a": "avg:cpu()"},
        "formula": "a"
    })json";

    TestGlazeDerivedQueryRequest parsed;
    auto ec = glz::read_json(parsed, jsonStr);
    ASSERT_FALSE(ec) << "Failed to parse minimal JSON: " << glz::format_error(ec);

    EXPECT_EQ(parsed.queries.size(), 1u);
    EXPECT_EQ(parsed.formula, "a");
    EXPECT_EQ(parsed.startTime, 0u);  // Default
    EXPECT_EQ(parsed.endTime, 0u);    // Default
}

TEST_F(HttpDerivedQueryHandlerTest, ParseJsonRequestManyQueries) {
    std::string jsonStr = R"json({
        "queries": {
            "a": "avg:cpu(usage)",
            "b": "avg:memory(used)",
            "c": "avg:disk(read_bytes)",
            "d": "avg:network(tx_bytes)"
        },
        "formula": "(a + b + c + d) / 4",
        "startTime": 1000,
        "endTime": 2000
    })json";

    TestGlazeDerivedQueryRequest parsed;
    auto ec = glz::read_json(parsed, jsonStr);
    ASSERT_FALSE(ec) << "Failed to parse multi-query JSON: " << glz::format_error(ec);

    EXPECT_EQ(parsed.queries.size(), 4u);
    EXPECT_GT(parsed.queries.count("a"), 0u);
    EXPECT_GT(parsed.queries.count("b"), 0u);
    EXPECT_GT(parsed.queries.count("c"), 0u);
    EXPECT_GT(parsed.queries.count("d"), 0u);
}

TEST_F(HttpDerivedQueryHandlerTest, ParseInvalidJson) {
    std::string jsonStr = "this is not json";

    TestGlazeDerivedQueryRequest parsed;
    auto ec = glz::read_json(parsed, jsonStr);
    EXPECT_TRUE(ec) << "Invalid JSON should fail to parse";
}

TEST_F(HttpDerivedQueryHandlerTest, ParseEmptyJsonObject) {
    std::string jsonStr = "{}";

    TestGlazeDerivedQueryRequest parsed;
    auto ec = glz::read_json(parsed, jsonStr);
    // Glaze will parse empty object with defaults
    ASSERT_FALSE(ec);

    EXPECT_TRUE(parsed.queries.empty());
    EXPECT_TRUE(parsed.formula.empty());
}

// =============================================================================
// DerivedQueryConfig Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, DefaultConfigValues) {
    DerivedQueryConfig config;

    EXPECT_EQ(config.alignmentStrategy, AlignmentStrategy::INNER);
    EXPECT_EQ(config.interpolationMethod, InterpolationMethod::LINEAR);
    EXPECT_EQ(config.maxSubQueries, 10u);
    EXPECT_EQ(config.maxTotalPoints, 10000000u);
    EXPECT_EQ(config.timeoutMs, 30000u);
}

TEST_F(HttpDerivedQueryHandlerTest, CustomConfig) {
    DerivedQueryConfig config;
    config.alignmentStrategy = AlignmentStrategy::OUTER;
    config.interpolationMethod = InterpolationMethod::ZERO;
    config.maxSubQueries = 5;
    config.maxTotalPoints = 1000;
    config.timeoutMs = 5000;

    EXPECT_EQ(config.alignmentStrategy, AlignmentStrategy::OUTER);
    EXPECT_EQ(config.interpolationMethod, InterpolationMethod::ZERO);
    EXPECT_EQ(config.maxSubQueries, 5u);
    EXPECT_EQ(config.maxTotalPoints, 1000u);
    EXPECT_EQ(config.timeoutMs, 5000u);
}

// =============================================================================
// DerivedQueryResult Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, DerivedQueryResultEmpty) {
    DerivedQueryResult result;
    EXPECT_TRUE(result.empty());
    EXPECT_EQ(result.size(), 0u);
}

TEST_F(HttpDerivedQueryHandlerTest, DerivedQueryResultWithData) {
    DerivedQueryResult result;
    result.timestamps = {1000, 2000, 3000};
    result.values = {10.0, 20.0, 30.0};

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.size(), 3u);
}

// =============================================================================
// SubQueryResult Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, SubQueryResultEmpty) {
    SubQueryResult result;
    EXPECT_TRUE(result.empty());
    EXPECT_EQ(result.size(), 0u);
}

TEST_F(HttpDerivedQueryHandlerTest, SubQueryResultWithData) {
    SubQueryResult result;
    result.queryName = "a";
    result.measurement = "cpu";
    result.field = "usage";
    result.timestamps = {1000, 2000};
    result.values = {50.0, 75.0};

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.size(), 2u);
    EXPECT_EQ(result.queryName, "a");
    EXPECT_EQ(result.measurement, "cpu");
    EXPECT_EQ(result.field, "usage");
}

// =============================================================================
// Formula Expression Integration Tests (parsing + validation)
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, ComplexFormulaValidation) {
    DerivedQueryRequest request;

    QueryRequest qA, qB, qC;
    qA.measurement = "cpu";
    qA.startTime = 1000;
    qA.endTime = 2000;
    qB.measurement = "memory";
    qB.startTime = 1000;
    qB.endTime = 2000;
    qC.measurement = "disk";
    qC.startTime = 1000;
    qC.endTime = 2000;

    request.queries["a"] = qA;
    request.queries["b"] = qB;
    request.queries["c"] = qC;
    request.formula = "(a * 0.5 + b * 0.3 + c * 0.2) / (a + b + c) * 100";

    EXPECT_NO_THROW(request.validate());
}

TEST_F(HttpDerivedQueryHandlerTest, FormulaWithFunctions) {
    DerivedQueryRequest request;

    QueryRequest qA, qB;
    qA.measurement = "cpu";
    qA.startTime = 1000;
    qA.endTime = 2000;
    qB.measurement = "memory";
    qB.startTime = 1000;
    qB.endTime = 2000;

    request.queries["a"] = qA;
    request.queries["b"] = qB;
    request.formula = "abs(a - b)";

    EXPECT_NO_THROW(request.validate());
}

TEST_F(HttpDerivedQueryHandlerTest, FormulaWithNestedFunctions) {
    DerivedQueryRequest request;

    QueryRequest qA;
    qA.measurement = "cpu";
    qA.startTime = 1000;
    qA.endTime = 2000;

    request.queries["a"] = qA;
    request.formula = "sqrt(abs(a))";

    EXPECT_NO_THROW(request.validate());
}

TEST_F(HttpDerivedQueryHandlerTest, FormulaWithScalarOnly) {
    // A formula that just references a single query with scalar multiplication
    DerivedQueryRequest request;

    QueryRequest qA;
    qA.measurement = "cpu";
    qA.startTime = 1000;
    qA.endTime = 2000;

    request.queries["a"] = qA;
    request.formula = "a * 100 + 50";

    EXPECT_NO_THROW(request.validate());
}

// =============================================================================
// DerivedQueryException Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, DerivedQueryExceptionMessage) {
    try {
        throw DerivedQueryException("Test error message");
    } catch (const DerivedQueryException& e) {
        EXPECT_EQ(std::string(e.what()), "Test error message");
    }
}

TEST_F(HttpDerivedQueryHandlerTest, DerivedQueryExceptionIsRuntimeError) {
    try {
        throw DerivedQueryException("Test");
    } catch (const std::runtime_error& e) {
        // Should be caught as runtime_error
        EXPECT_EQ(std::string(e.what()), "Test");
    }
}

// =============================================================================
// Handler Construction Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, ConstructWithNullEngineThrows) {
    // Null engine must throw std::invalid_argument to prevent later segfault
    EXPECT_THROW(HttpDerivedQueryHandler(nullptr), std::invalid_argument);
}

TEST_F(HttpDerivedQueryHandlerTest, ConstructWithNullIndexAccepted) {
    // Null index is valid — the server always passes null since metadata
    // lookups go through the engine. Verify construction does not throw.
    auto* fakeEngine = reinterpret_cast<seastar::sharded<Engine>*>(uintptr_t{1});
    EXPECT_NO_THROW(HttpDerivedQueryHandler(fakeEngine, nullptr));
}

// =============================================================================
// Executor Construction Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, ExecutorConstructWithNullEngine) {
    EXPECT_NO_THROW(DerivedQueryExecutor(nullptr));
}

TEST_F(HttpDerivedQueryHandlerTest, ExecutorConstructWithCustomConfig) {
    DerivedQueryConfig config;
    config.maxSubQueries = 20;
    config.maxTotalPoints = 5000000;

    EXPECT_NO_THROW(DerivedQueryExecutor(nullptr, nullptr, config));
}

// =============================================================================
// JSON Response Round-Trip Tests
// =============================================================================

TEST_F(HttpDerivedQueryHandlerTest, ErrorResponseRoundTrip) {
    // Create error -> parse -> verify
    std::string json = DerivedQueryExecutor::createErrorResponse(
        "PARSE_ERROR", "Missing formula field");

    TestDerivedResponse parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    EXPECT_EQ(parsed.status, "error");
    EXPECT_EQ(parsed.error.code, "PARSE_ERROR");
    EXPECT_EQ(parsed.error.message, "Missing formula field");
    // Success response fields should be empty/default
    EXPECT_TRUE(parsed.timestamps.empty());
    EXPECT_TRUE(parsed.values.empty());
}

TEST_F(HttpDerivedQueryHandlerTest, SuccessResponseRoundTrip) {
    DerivedQueryResult result;
    result.formula = "a / b * 100";
    result.timestamps = {100, 200, 300, 400, 500};
    result.values = {10.0, 20.0, 30.0, 40.0, 50.0};
    result.stats.pointCount = 5;
    result.stats.executionTimeMs = 7.5;
    result.stats.subQueriesExecuted = 2;
    result.stats.pointsDroppedDueToAlignment = 3;

    std::string json = executor.formatResponse(result);

    TestDerivedResponse parsed;
    auto ec = glz::read_json(parsed, json);
    ASSERT_FALSE(ec);

    EXPECT_EQ(parsed.status, "success");
    EXPECT_EQ(parsed.formula, "a / b * 100");
    ASSERT_EQ(parsed.timestamps.size(), 5u);
    ASSERT_EQ(parsed.values.size(), 5u);

    for (size_t i = 0; i < 5; ++i) {
        EXPECT_EQ(parsed.timestamps[i], (i + 1) * 100);
        EXPECT_DOUBLE_EQ(parsed.values[i], (i + 1) * 10.0);
    }

    EXPECT_EQ(parsed.statistics.point_count, 5u);
    EXPECT_DOUBLE_EQ(parsed.statistics.execution_time_ms, 7.5);
    EXPECT_EQ(parsed.statistics.sub_queries_executed, 2u);
    EXPECT_EQ(parsed.statistics.points_dropped_due_to_alignment, 3u);
}
