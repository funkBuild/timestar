// Seastar-based async tests for DerivedQueryExecutor
// Tests execute(), executeWithAnomaly(), and executeForecast() with a real
// sharded Engine, verifying sub-query fan-out, formula evaluation, alignment,
// and error propagation under the Seastar reactor.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/query/derived_query.hpp"
#include "../../../lib/query/derived_query_executor.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cmath>
#include <filesystem>
#include <limits>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <string>
#include <vector>

namespace fs = std::filesystem;

using namespace timestar;

class DerivedQueryExecutorSeastarTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }

    void TearDown() override { cleanTestShardDirectories(); }
};

// ---------------------------------------------------------------------------
// Helper: insert float data via the shardedInsert helper
// ---------------------------------------------------------------------------
static void insertFloatSeries(seastar::sharded<Engine>& eng, const std::string& measurement, const std::string& field,
                              const std::map<std::string, std::string>& tags,
                              const std::vector<std::pair<uint64_t, double>>& points) {
    TimeStarInsert<double> insert(measurement, field);
    for (const auto& [k, v] : tags) {
        insert.addTag(k, v);
    }
    for (const auto& [ts, val] : points) {
        insert.addValue(ts, val);
    }
    shardedInsert(eng, std::move(insert));
}

// ===========================================================================
// 1. Basic derived query: single sub-query, identity formula
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, SingleSubQueryIdentityFormula) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert CPU data with realistic timestamps
        uint64_t startNs = 1704067200000000000ULL;
        uint64_t intervalNs = 60000000000ULL;
        std::vector<std::pair<uint64_t, double>> cpuPoints;
        for (size_t i = 0; i < 5; ++i) {
            cpuPoints.push_back({startNs + i * intervalNs, static_cast<double>((i + 1) * 10)});
        }
        insertFloatSeries(eng.eng, "cpu", "usage", {{"host", "s1"}}, cpuPoints);

        // Build derived query: just pass-through "a"
        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "a";
        request.startTime = startNs;
        request.endTime = startNs + 10 * intervalNs;

        QueryRequest qr;
        qr.measurement = "cpu";
        qr.fields = {"usage"};
        qr.scopes = {{"host", "s1"}};
        qr.startTime = startNs;
        qr.endTime = startNs + 10 * intervalNs;
        request.queries["a"] = qr;

        auto result = executor.execute(request).get();

        EXPECT_EQ(result.formula, "a");
        EXPECT_EQ(result.stats.subQueriesExecuted, 1u);
        EXPECT_EQ(result.timestamps.size(), result.values.size());

        // Should have retrieved the 5 inserted points
        EXPECT_EQ(result.timestamps.size(), 5u);
        EXPECT_DOUBLE_EQ(result.values[0], 10.0);
        EXPECT_DOUBLE_EQ(result.values[4], 50.0);
    })
        .join()
        .get();
}

// ===========================================================================
// 2. Arithmetic formula across two sub-queries
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, TwoSubQueryArithmeticFormula) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert two series with matching timestamps
        uint64_t startNs = 1704067200000000000ULL;
        uint64_t intervalNs = 60000000000ULL;
        std::vector<std::pair<uint64_t, double>> cpuPoints;
        std::vector<std::pair<uint64_t, double>> memPoints;
        for (size_t i = 0; i < 3; ++i) {
            cpuPoints.push_back({startNs + i * intervalNs, 50.0});
            memPoints.push_back({startNs + i * intervalNs, 200.0});
        }
        insertFloatSeries(eng.eng, "sys", "cpu", {}, cpuPoints);
        insertFloatSeries(eng.eng, "sys", "mem", {}, memPoints);

        // Formula: cpu / mem * 100 = 50 / 200 * 100 = 25
        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "a / b * 100";
        request.startTime = startNs;
        request.endTime = startNs + 10 * intervalNs;

        {
            QueryRequest qr;
            qr.measurement = "sys";
            qr.fields = {"cpu"};
            qr.startTime = startNs;
            qr.endTime = startNs + 10 * intervalNs;
            request.queries["a"] = qr;
        }
        {
            QueryRequest qr;
            qr.measurement = "sys";
            qr.fields = {"mem"};
            qr.startTime = startNs;
            qr.endTime = startNs + 10 * intervalNs;
            request.queries["b"] = qr;
        }

        auto result = executor.execute(request).get();

        EXPECT_EQ(result.stats.subQueriesExecuted, 2u);
        EXPECT_EQ(result.timestamps.size(), 3u);

        for (size_t i = 0; i < result.values.size(); ++i) {
            EXPECT_NEAR(result.values[i], 25.0, 0.01) << "Formula evaluation mismatch at index " << i;
        }
    })
        .join()
        .get();
}

// ===========================================================================
// 3. Execute with empty sub-query result returns empty
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, EmptySubQueryResult) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Don't insert any data -- query should return empty
        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "a";
        request.startTime = 1000;
        request.endTime = 5000;

        QueryRequest qr;
        qr.measurement = "nonexistent";
        qr.fields = {"field"};
        qr.startTime = 1000;
        qr.endTime = 5000;
        request.queries["a"] = qr;

        auto result = executor.execute(request).get();

        EXPECT_TRUE(result.empty());
        EXPECT_EQ(result.timestamps.size(), 0u);
    })
        .join()
        .get();
}

// ===========================================================================
// 4. Validation: too many sub-queries
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, TooManySubQueriesThrows) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        DerivedQueryConfig config;
        config.maxSubQueries = 2;
        DerivedQueryExecutor executor(&eng.eng, config);

        DerivedQueryRequest request;
        request.formula = "a + b + c";
        request.startTime = 1000;
        request.endTime = 2000;

        for (const auto& name : {"a", "b", "c"}) {
            QueryRequest qr;
            qr.measurement = "m";
            qr.fields = {"f"};
            qr.startTime = 1000;
            qr.endTime = 2000;
            request.queries[name] = qr;
        }

        EXPECT_THROW(executor.execute(request).get(), DerivedQueryException);
    })
        .join()
        .get();
}

// ===========================================================================
// 5. Invalid formula throws
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, InvalidFormulaThrows) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "";  // empty formula
        request.queries["a"] = QueryRequest();

        EXPECT_THROW(executor.execute(request).get(), DerivedQueryException);
    })
        .join()
        .get();
}

// ===========================================================================
// 6. executeFromJson round-trip
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, ExecuteFromJsonRoundTrip) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert data with realistic timestamps
        uint64_t startNs = 1704067200000000000ULL;
        uint64_t intervalNs = 60000000000ULL;
        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < 3; ++i) {
            points.push_back({startNs + i * intervalNs, 42.0});
        }
        insertFloatSeries(eng.eng, "temp", "value", {{"loc", "west"}}, points);

        DerivedQueryExecutor executor(&eng.eng);

        std::string json = R"json({
            "queries": {
                "a": "avg:temp(value){loc:west}"
            },
            "formula": "a * 2",
            "startTime": 1704067200000000000,
            "endTime": 1704073200000000000
        })json";

        auto result = executor.executeFromJson(json).get();

        EXPECT_EQ(result.formula, "a * 2");
        EXPECT_EQ(result.timestamps.size(), 3u);

        for (double v : result.values) {
            EXPECT_NEAR(v, 84.0, 0.01);
        }
    })
        .join()
        .get();
}

// ===========================================================================
// 7. executeFromJson with invalid JSON throws
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, ExecuteFromJsonInvalidJsonThrows) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        DerivedQueryExecutor executor(&eng.eng);

        std::string badJson = "{ not valid json";
        EXPECT_THROW(executor.executeFromJson(badJson).get(), DerivedQueryException);
    })
        .join()
        .get();
}

// ===========================================================================
// 8. Format response round-trip
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, FormatResponseRoundTrip) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        uint64_t startNs = 1704067200000000000ULL;
        uint64_t intervalNs = 60000000000ULL;
        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < 3; ++i) {
            points.push_back({startNs + i * intervalNs, 10.0});
        }
        insertFloatSeries(eng.eng, "metric", "val", {}, points);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "a";
        request.startTime = startNs;
        request.endTime = startNs + 10 * intervalNs;

        QueryRequest qr;
        qr.measurement = "metric";
        qr.fields = {"val"};
        qr.startTime = startNs;
        qr.endTime = startNs + 10 * intervalNs;
        request.queries["a"] = qr;

        auto result = executor.execute(request).get();
        auto jsonResponse = executor.formatResponse(result);

        // Should be valid JSON containing "success"
        EXPECT_TRUE(jsonResponse.find("\"success\"") != std::string::npos);
        EXPECT_TRUE(jsonResponse.find("\"timestamps\"") != std::string::npos);
        EXPECT_TRUE(jsonResponse.find("\"values\"") != std::string::npos);
    })
        .join()
        .get();
}

// ===========================================================================
// 9. executeWithAnomaly dispatches to regular path for non-anomaly formula
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, ExecuteWithAnomalyRegularFormula) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        uint64_t startNs = 1704067200000000000ULL;
        uint64_t intervalNs = 60000000000ULL;
        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < 3; ++i) {
            points.push_back({startNs + i * intervalNs, 10.0});
        }
        insertFloatSeries(eng.eng, "metric", "val", {}, points);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "a + 5";
        request.startTime = startNs;
        request.endTime = startNs + 10 * intervalNs;

        QueryRequest qr;
        qr.measurement = "metric";
        qr.fields = {"val"};
        qr.startTime = startNs;
        qr.endTime = startNs + 10 * intervalNs;
        request.queries["a"] = qr;

        auto variantResult = executor.executeWithAnomaly(request).get();

        // Should be a regular DerivedQueryResult (not anomaly/forecast)
        ASSERT_TRUE(std::holds_alternative<DerivedQueryResult>(variantResult));
        auto& result = std::get<DerivedQueryResult>(variantResult);
        EXPECT_EQ(result.timestamps.size(), 3u);
        for (double v : result.values) {
            EXPECT_NEAR(v, 15.0, 0.01);
        }
    })
        .join()
        .get();
}

// ===========================================================================
// 10. executeWithAnomaly dispatches to anomaly path
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, ExecuteWithAnomalyDetection) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert 100 data points for anomaly detection to work
        std::vector<std::pair<uint64_t, double>> points;
        uint64_t intervalNs = 60000000000ULL;  // 1 minute
        uint64_t startNs = 1704067200000000000ULL;
        for (size_t i = 0; i < 100; ++i) {
            double val = 50.0;
            if (i == 75)
                val = 200.0;  // anomaly
            points.push_back({startNs + i * intervalNs, val});
        }
        insertFloatSeries(eng.eng, "cpu", "usage", {{"host", "s1"}}, points);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "anomalies(a, 'basic', 2)";
        request.startTime = startNs;
        request.endTime = startNs + 100 * intervalNs;

        QueryRequest qr;
        qr.measurement = "cpu";
        qr.fields = {"usage"};
        qr.scopes = {{"host", "s1"}};
        qr.startTime = request.startTime;
        qr.endTime = request.endTime;
        request.queries["a"] = qr;

        auto variantResult = executor.executeWithAnomaly(request).get();

        // Should dispatch to anomaly result
        ASSERT_TRUE(std::holds_alternative<anomaly::AnomalyQueryResult>(variantResult));
        auto& anomalyResult = std::get<anomaly::AnomalyQueryResult>(variantResult);

        EXPECT_TRUE(anomalyResult.success);
        EXPECT_EQ(anomalyResult.times.size(), 100u);
        // Should have series pieces: raw, upper, lower, scores (at least 4)
        EXPECT_GE(anomalyResult.series.size(), 4u);

        // Statistics
        EXPECT_EQ(anomalyResult.statistics.algorithm, "basic");
        EXPECT_EQ(anomalyResult.statistics.totalPoints, 100u);
    })
        .join()
        .get();
}

// ===========================================================================
// 11. executeWithAnomaly dispatches to forecast path
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, ExecuteWithForecast) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert trending data for forecasting
        std::vector<std::pair<uint64_t, double>> points;
        uint64_t intervalNs = 60000000000ULL;
        uint64_t startNs = 1704067200000000000ULL;
        for (size_t i = 0; i < 100; ++i) {
            double val = 10.0 + 0.5 * i;
            points.push_back({startNs + i * intervalNs, val});
        }
        insertFloatSeries(eng.eng, "metric", "load", {{"dc", "east"}}, points);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "forecast(a, 'linear', 2)";
        // Set the time range such that forecastHorizon can be computed
        request.startTime = startNs;
        request.endTime = startNs + 200 * intervalNs;  // extended range for forecast

        QueryRequest qr;
        qr.measurement = "metric";
        qr.fields = {"load"};
        qr.scopes = {{"dc", "east"}};
        qr.startTime = startNs;
        qr.endTime = startNs + 100 * intervalNs;
        request.queries["a"] = qr;

        auto variantResult = executor.executeWithAnomaly(request).get();

        // Should dispatch to forecast result
        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variantResult));
        auto& forecastResult = std::get<forecast::ForecastQueryResult>(variantResult);

        EXPECT_TRUE(forecastResult.success);
        EXPECT_FALSE(forecastResult.empty());
        // Should have series pieces: past, forecast, upper, lower (at least 4)
        EXPECT_GE(forecastResult.series.size(), 4u);

        // Statistics
        EXPECT_EQ(forecastResult.statistics.algorithm, "linear");
        EXPECT_GT(forecastResult.statistics.historicalPoints, 0u);
        EXPECT_GT(forecastResult.statistics.forecastPoints, 0u);
    })
        .join()
        .get();
}

// ===========================================================================
// 12. formatResponseVariant for all three types
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, FormatResponseVariantAllTypes) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        DerivedQueryExecutor executor(&eng.eng);

        // Regular result
        {
            DerivedQueryResult result;
            result.timestamps = {1000, 2000};
            result.values = {1.0, 2.0};
            result.formula = "a";

            DerivedQueryResultVariant variant{result};
            auto json = executor.formatResponseVariant(variant);
            EXPECT_TRUE(json.find("\"success\"") != std::string::npos);
        }

        // Anomaly result
        {
            anomaly::AnomalyQueryResult result;
            result.success = true;
            result.times = {1000, 2000};
            result.statistics.algorithm = "basic";
            result.statistics.bounds = 2.0;
            result.statistics.totalPoints = 2;

            DerivedQueryResultVariant variant{result};
            auto json = executor.formatResponseVariant(variant);
            EXPECT_TRUE(json.find("\"success\"") != std::string::npos);
            EXPECT_TRUE(json.find("\"basic\"") != std::string::npos);
        }

        // Forecast result
        {
            forecast::ForecastQueryResult result;
            result.success = true;
            result.times = {1000, 2000, 3000};
            result.forecastStartIndex = 2;
            result.statistics.algorithm = "linear";

            DerivedQueryResultVariant variant{result};
            auto json = executor.formatResponseVariant(variant);
            EXPECT_TRUE(json.find("\"success\"") != std::string::npos);
            EXPECT_TRUE(json.find("\"linear\"") != std::string::npos);
        }
    })
        .join()
        .get();
}

// ===========================================================================
// 13. executeFromJsonWithAnomaly anomaly path
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, ExecuteFromJsonWithAnomalyPath) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert stable data with an outlier
        std::vector<std::pair<uint64_t, double>> points;
        uint64_t intervalNs = 60000000000ULL;
        uint64_t startNs = 1704067200000000000ULL;
        for (size_t i = 0; i < 100; ++i) {
            double val = 50.0;
            if (i == 80)
                val = 300.0;
            points.push_back({startNs + i * intervalNs, val});
        }
        insertFloatSeries(eng.eng, "load", "cpu", {{"dc", "us"}}, points);

        DerivedQueryExecutor executor(&eng.eng);

        // Use a very large endTime to avoid overflow
        std::string json = R"json({
            "queries": {
                "q": "avg:load(cpu){dc:us}"
            },
            "formula": "anomalies(q, 'basic', 2)",
            "startTime": 1704067200000000000,
            "endTime": 1704073200000000000
        })json";

        auto variantResult = executor.executeFromJsonWithAnomaly(json).get();

        ASSERT_TRUE(std::holds_alternative<anomaly::AnomalyQueryResult>(variantResult));
        auto& anomalyResult = std::get<anomaly::AnomalyQueryResult>(variantResult);
        EXPECT_TRUE(anomalyResult.success);
        EXPECT_GE(anomalyResult.series.size(), 4u);
    })
        .join()
        .get();
}

// ===========================================================================
// 14. Query with time-range filtering in derived executor
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, TimeRangeFilteredSubQuery) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert 10 points
        std::vector<std::pair<uint64_t, double>> points;
        for (uint64_t t = 1000; t <= 10000; t += 1000) {
            points.push_back({t, static_cast<double>(t)});
        }
        insertFloatSeries(eng.eng, "metric", "val", {}, points);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "a";
        request.startTime = 3000;
        request.endTime = 7000;

        QueryRequest qr;
        qr.measurement = "metric";
        qr.fields = {"val"};
        qr.startTime = 3000;
        qr.endTime = 7000;
        request.queries["a"] = qr;

        auto result = executor.execute(request).get();

        // Should get 5 points: 3000, 4000, 5000, 6000, 7000
        EXPECT_EQ(result.timestamps.size(), 5u);
        EXPECT_EQ(result.timestamps.front(), 3000u);
        EXPECT_EQ(result.timestamps.back(), 7000u);
    })
        .join()
        .get();
}

// ===========================================================================
// 15. Unused queries are not executed
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, UnusedQueriesNotExecuted) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        uint64_t startNs = 1704067200000000000ULL;
        uint64_t intervalNs = 60000000000ULL;
        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < 3; ++i) {
            points.push_back({startNs + i * intervalNs, 10.0});
        }
        insertFloatSeries(eng.eng, "metric", "val", {}, points);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "a";  // Only references "a", not "unused"
        request.startTime = startNs;
        request.endTime = startNs + 10 * intervalNs;

        {
            QueryRequest qr;
            qr.measurement = "metric";
            qr.fields = {"val"};
            qr.startTime = startNs;
            qr.endTime = startNs + 10 * intervalNs;
            request.queries["a"] = qr;
        }
        {
            // This query is defined but not referenced in the formula
            QueryRequest qr;
            qr.measurement = "nonexistent";
            qr.fields = {"x"};
            qr.startTime = startNs;
            qr.endTime = startNs + 10 * intervalNs;
            request.queries["unused"] = qr;
        }

        auto result = executor.execute(request).get();

        // Only "a" should have been executed
        EXPECT_EQ(result.stats.subQueriesExecuted, 1u);
        EXPECT_EQ(result.timestamps.size(), 3u);
    })
        .join()
        .get();
}

// ===========================================================================
// 16. Anomaly with empty sub-query returns success with empty result
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, AnomalyEmptySubQueryReturnsEmpty) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "anomalies(a, 'basic', 2)";
        request.startTime = 1000;
        request.endTime = 5000;

        QueryRequest qr;
        qr.measurement = "nonexistent";
        qr.fields = {"f"};
        qr.startTime = 1000;
        qr.endTime = 5000;
        request.queries["a"] = qr;

        auto variantResult = executor.executeWithAnomaly(request).get();

        ASSERT_TRUE(std::holds_alternative<anomaly::AnomalyQueryResult>(variantResult));
        auto& result = std::get<anomaly::AnomalyQueryResult>(variantResult);
        EXPECT_TRUE(result.success);
        EXPECT_TRUE(result.empty());
    })
        .join()
        .get();
}

// ===========================================================================
// 17. Forecast with empty sub-query returns success with empty result
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, ForecastEmptySubQueryReturnsEmpty) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "forecast(a, 'linear', 2)";
        request.startTime = 1000;
        request.endTime = 10000;

        QueryRequest qr;
        qr.measurement = "nonexistent";
        qr.fields = {"f"};
        qr.startTime = 1000;
        qr.endTime = 10000;
        request.queries["a"] = qr;

        auto variantResult = executor.executeWithAnomaly(request).get();

        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variantResult));
        auto& result = std::get<forecast::ForecastQueryResult>(variantResult);
        EXPECT_TRUE(result.success);
        EXPECT_TRUE(result.empty());
    })
        .join()
        .get();
}

// ===========================================================================
// 18. REGRESSION: aggregationInterval must propagate to sub-queries
//
// Bug: DerivedQueryExecutor never copied the request-level
// aggregationInterval into the sub-query QueryRequests, so sub-queries ran
// with interval == 0.  On the interval-0 aggregation pushdown path (TSM
// pushdown and its MemoryStore fold for freshly-ingested data) each
// sub-query collapsed to a single point, while the identical plain /query
// with the same interval returned one point per bucket.  The derived result
// must have the same bucket count as the equivalent plain query, on every
// storage tier.
// ===========================================================================

// Helper for the regression test: bucket count of a plain query with the
// given interval (the reference the derived query must match).
static size_t plainQueryBucketCount(seastar::sharded<Engine>& eng, const std::string& field, uint64_t startNs,
                                    uint64_t endNs, uint64_t intervalNs) {
    http::HttpQueryHandler handler(&eng);
    QueryRequest plain;
    plain.aggregation = AggregationMethod::AVG;
    plain.measurement = "server.metrics";
    plain.fields = {field};
    plain.scopes = {{"host", "h1"}};
    plain.startTime = startNs;
    plain.endTime = endNs;
    plain.aggregationInterval = intervalNs;
    auto response = handler.executeQuery(plain).get();

    EXPECT_TRUE(response.success);
    EXPECT_EQ(response.series.size(), 1u);
    if (response.series.size() != 1u) {
        return 0;
    }
    return response.series[0].fields.at(field).first.size();
}

// Helper for the regression test: run the derived query through the JSON
// entry point (the /derived body path) and verify bucket count and values.
static void expectDerivedMatchesPlainBuckets(seastar::sharded<Engine>& eng, uint64_t startNs, uint64_t endNs,
                                             uint64_t fiveMinNs, size_t plainBuckets, const char* phase) {
    DerivedQueryExecutor executor(&eng);
    std::string json = R"json({
        "queries": {
            "a": "avg:server.metrics(cpu){host:h1}",
            "b": "avg:server.metrics(mem){host:h1}"
        },
        "formula": "(a + b) / 2",
        "startTime": )json" +
                       std::to_string(startNs) +
                       R"json(,
        "endTime": )json" +
                       std::to_string(endNs) +
                       R"json(,
        "aggregationInterval": "5m"
    })json";

    auto result = executor.executeFromJson(json).get();

    // The exact regression: derived returned 1 collapsed point instead of
    // one point per bucket.
    EXPECT_EQ(result.timestamps.size(), plainBuckets)
        << phase << ": derived query bucket count must match the equivalent plain query";
    EXPECT_EQ(result.values.size(), result.timestamps.size());

    if (result.timestamps.size() == plainBuckets && plainBuckets > 0) {
        // Bucket-start timestamps aligned to the interval grid.
        EXPECT_EQ(result.timestamps.front(), startNs) << phase;
        EXPECT_EQ(result.timestamps.back(), startNs + (plainBuckets - 1) * fiveMinNs) << phase;
    }

    // (a + b) / 2 = (50 + 200) / 2 = 125 in every bucket.
    for (double v : result.values) {
        EXPECT_NEAR(v, 125.0, 0.01) << phase;
    }
}

TEST_F(DerivedQueryExecutorSeastarTest, AggregationIntervalBucketsFreshData) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;  // multiple of 5m
        const uint64_t minuteNs = 60000000000ULL;
        const uint64_t fiveMinNs = 300000000000ULL;
        constexpr size_t kPointsPerPhase = 150;  // 150 minutes per phase

        auto insertRange = [&](size_t firstMinute, size_t count) {
            std::vector<std::pair<uint64_t, double>> cpuPoints;
            std::vector<std::pair<uint64_t, double>> memPoints;
            for (size_t i = firstMinute; i < firstMinute + count; ++i) {
                cpuPoints.push_back({startNs + i * minuteNs, 50.0});
                memPoints.push_back({startNs + i * minuteNs, 200.0});
            }
            insertFloatSeries(eng.eng, "server.metrics", "cpu", {{"host", "h1"}}, cpuPoints);
            insertFloatSeries(eng.eng, "server.metrics", "mem", {{"host", "h1"}}, memPoints);
        };

        // ---- Phase 1: MemoryStore-only (freshly-ingested, no rollover) ----
        insertRange(0, kPointsPerPhase);
        uint64_t phase1End = startNs + kPointsPerPhase * minuteNs;

        size_t plainBuckets = plainQueryBucketCount(eng.eng, "cpu", startNs, phase1End, fiveMinNs);
        EXPECT_EQ(plainBuckets, kPointsPerPhase / 5) << "Plain query should return one point per 5m bucket";
        expectDerivedMatchesPlainBuckets(eng.eng, startNs, phase1End, fiveMinNs, plainBuckets, "memory-only");

        // ---- Phase 2: TSM files + fresh MemoryStore tail ----
        // Roll the first 150 minutes over to TSM, then ingest 150 more
        // minutes that stay in the MemoryStore.  This is the live-repro
        // condition: sub-queries take the TSM pushdown + MemoryStore fold
        // path, which collapsed to one point when the interval was dropped.
        eng.eng.invoke_on_all([](Engine& engine) { return engine.rolloverMemoryStore(); }).get();
        seastar::sleep(std::chrono::milliseconds(300)).get();  // background TSM conversion

        insertRange(kPointsPerPhase, kPointsPerPhase);
        uint64_t phase2End = startNs + 2 * kPointsPerPhase * minuteNs;

        plainBuckets = plainQueryBucketCount(eng.eng, "cpu", startNs, phase2End, fiveMinNs);
        EXPECT_EQ(plainBuckets, 2 * kPointsPerPhase / 5) << "Plain query should return one point per 5m bucket";
        expectDerivedMatchesPlainBuckets(eng.eng, startNs, phase2End, fiveMinNs, plainBuckets, "tsm+memory");
    })
        .join()
        .get();
}

// ===========================================================================
// 18b. REGRESSION: aggregationInterval must reach forecast()/anomalies()
//
// Bug: executeForecast() and executeAnomalyDetection() called executeSubQuery()
// with the raw leg straight out of request.queries, bypassing the propagation
// executeAllSubQueries() applies for arithmetic formulas.  So a /derived
// forecast ran at RAW resolution whatever aggregationInterval the caller sent
// -- 2880 hourly points where the identical request with formula `a * 1`
// returned 120 daily buckets -- and running the forecast with an interval and
// with no interval at all produced byte-identical output.
//
// Consequence of the fix worth stating: the forecast horizon is derived from
// the leg's sampling interval, so honouring the parameter also changes the
// projected point count (one per bucket, not one per raw sample).
// ===========================================================================

// Build a /derived JSON body for one leg over server.metrics(cpu).
// `intervalLiteral` is spliced in verbatim so a test can exercise the JSON
// number form as well as the string forms; nullptr omits the key entirely.
static std::string derivedJsonBody(const std::string& formula, uint64_t startNs, uint64_t endNs,
                                   const char* intervalLiteral) {
    std::string json = R"json({"queries":{"a":"avg:server.metrics(cpu){host:h1}"},"formula":")json" + formula +
                       R"json(","startTime":)json" + std::to_string(startNs) + R"json(,"endTime":)json" +
                       std::to_string(endNs);
    if (intervalLiteral != nullptr) {
        json += R"json(,"aggregationInterval":)json";
        json += intervalLiteral;
    }
    json += "}";
    return json;
}

TEST_F(DerivedQueryExecutorSeastarTest, AggregationIntervalReachesForecastAndAnomalies) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;  // multiple of 5m
        const uint64_t minuteNs = 60000000000ULL;
        const uint64_t fiveMinNs = 300000000000ULL;
        constexpr size_t kRawPoints = 150;  // 150 one-minute samples -> 30 5m buckets

        std::vector<std::pair<uint64_t, double>> cpuPoints;
        for (size_t i = 0; i < kRawPoints; ++i) {
            // A clean ramp so the linear fit is well defined at both resolutions.
            cpuPoints.push_back({startNs + i * minuteNs, static_cast<double>(i)});
        }
        insertFloatSeries(eng.eng, "server.metrics", "cpu", {{"host", "h1"}}, cpuPoints);

        const uint64_t endNs = startNs + kRawPoints * minuteNs;

        // Reference: the equivalent plain /query with the same interval.
        const size_t plainBuckets = plainQueryBucketCount(eng.eng, "cpu", startNs, endNs, fiveMinNs);
        ASSERT_EQ(plainBuckets, kRawPoints / 5) << "Plain query should return one point per 5m bucket";

        // ---- forecast() honours the interval ----
        {
            DerivedQueryExecutor executor(&eng.eng);
            auto variant =
                executor
                    .executeFromJsonWithAnomaly(derivedJsonBody("forecast(a, 'linear', 2)", startNs, endNs, R"("5m")"))
                    .get();

            ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
            const auto& result = std::get<forecast::ForecastQueryResult>(variant);
            EXPECT_TRUE(result.success);
            EXPECT_EQ(result.statistics.historicalPoints, plainBuckets)
                << "REGRESSION: forecast() ignored aggregationInterval and fitted the raw series";
            // Horizon follows the bucketed sampling interval, not the raw one.
            EXPECT_LT(result.statistics.forecastPoints, kRawPoints)
                << "REGRESSION: forecast horizon was computed from the raw sampling interval";
        }

        // ---- anomalies() honours the interval ----
        {
            DerivedQueryExecutor executor(&eng.eng);
            auto variant =
                executor
                    .executeFromJsonWithAnomaly(derivedJsonBody("anomalies(a, 'basic', 2)", startNs, endNs, R"("5m")"))
                    .get();

            ASSERT_TRUE(std::holds_alternative<anomaly::AnomalyQueryResult>(variant));
            const auto& result = std::get<anomaly::AnomalyQueryResult>(variant);
            EXPECT_TRUE(result.success);
            EXPECT_EQ(result.statistics.totalPoints, plainBuckets)
                << "REGRESSION: anomalies() ignored aggregationInterval and scanned the raw series";
            EXPECT_EQ(result.times.size(), plainBuckets);
        }

        // ---- and the parameter actually changes the answer ----
        // Omitting it must fall back to the raw series; the bug made these two
        // requests byte-identical.
        {
            DerivedQueryExecutor executor(&eng.eng);
            auto variant =
                executor
                    .executeFromJsonWithAnomaly(derivedJsonBody("forecast(a, 'linear', 2)", startNs, endNs, nullptr))
                    .get();

            ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
            const auto& result = std::get<forecast::ForecastQueryResult>(variant);
            EXPECT_EQ(result.statistics.historicalPoints, kRawPoints)
                << "Without an interval the forecast must still see every raw point";
        }
    })
        .join()
        .get();
}

// ===========================================================================
// 18c. aggregationInterval accepts a JSON number, a duration string, and a
// bare numeric string -- all meaning the same thing.
//
// Bug: GlazeDerivedQueryRequest typed the field as std::string, so the JSON
// numeric form documented for every other transport
// ("300000000000" == 300000000000 == "300000000000ns") was rejected outright
// with HTTP 400 `Invalid JSON: expected_quote` on /derived alone.
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, AggregationIntervalAcceptsNumericAndStringForms) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;  // multiple of 5m
        const uint64_t minuteNs = 60000000000ULL;
        const uint64_t fiveMinNs = 300000000000ULL;
        constexpr size_t kRawPoints = 150;

        std::vector<std::pair<uint64_t, double>> cpuPoints;
        for (size_t i = 0; i < kRawPoints; ++i) {
            cpuPoints.push_back({startNs + i * minuteNs, static_cast<double>(i)});
        }
        insertFloatSeries(eng.eng, "server.metrics", "cpu", {{"host", "h1"}}, cpuPoints);

        const uint64_t endNs = startNs + kRawPoints * minuteNs;
        const size_t plainBuckets = plainQueryBucketCount(eng.eng, "cpu", startNs, endNs, fiveMinNs);
        ASSERT_EQ(plainBuckets, kRawPoints / 5);

        // All three spellings of the same 5-minute interval.
        const char* spellings[] = {"300000000000", R"("5m")", R"("300000000000")"};

        // Entry point 1: executeFromJson() (arithmetic formula).
        std::vector<uint64_t> referenceTimestamps;
        std::vector<double> referenceValues;
        for (const char* spelling : spellings) {
            DerivedQueryExecutor executor(&eng.eng);
            auto result = executor.executeFromJson(derivedJsonBody("a * 1", startNs, endNs, spelling)).get();

            EXPECT_EQ(result.timestamps.size(), plainBuckets) << "spelling=" << spelling;
            EXPECT_EQ(result.values.size(), result.timestamps.size()) << "spelling=" << spelling;

            if (referenceTimestamps.empty()) {
                referenceTimestamps = result.timestamps;
                referenceValues = result.values;
            } else {
                EXPECT_EQ(result.timestamps, referenceTimestamps) << "spelling=" << spelling;
                EXPECT_EQ(result.values, referenceValues) << "spelling=" << spelling;
            }
        }

        // Entry point 2: executeFromJsonWithAnomaly() parses the interval on a
        // separate code path, so it needs the same coverage.
        size_t referenceHistorical = 0;
        for (const char* spelling : spellings) {
            DerivedQueryExecutor executor(&eng.eng);
            auto variant =
                executor
                    .executeFromJsonWithAnomaly(derivedJsonBody("forecast(a, 'linear', 2)", startNs, endNs, spelling))
                    .get();

            ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant)) << "spelling=" << spelling;
            const auto& result = std::get<forecast::ForecastQueryResult>(variant);
            EXPECT_TRUE(result.success) << "spelling=" << spelling;
            EXPECT_EQ(result.statistics.historicalPoints, plainBuckets) << "spelling=" << spelling;

            if (referenceHistorical == 0) {
                referenceHistorical = result.statistics.historicalPoints;
            } else {
                EXPECT_EQ(result.statistics.historicalPoints, referenceHistorical) << "spelling=" << spelling;
            }
        }
    })
        .join()
        .get();
}

// ===========================================================================
// 19. isAnomalyFormula / isForecastFormula static helpers
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, StaticFormulaDetectors) {
    EXPECT_TRUE(DerivedQueryExecutor::isAnomalyFormula("anomalies(a, 'basic', 2)"));
    EXPECT_TRUE(DerivedQueryExecutor::isAnomalyFormula("  anomalies(q, 'robust', 3)"));
    EXPECT_FALSE(DerivedQueryExecutor::isAnomalyFormula("a + b"));
    EXPECT_FALSE(DerivedQueryExecutor::isAnomalyFormula("forecast(a, 'linear', 2)"));

    EXPECT_TRUE(DerivedQueryExecutor::isForecastFormula("forecast(a, 'linear', 2)"));
    EXPECT_TRUE(DerivedQueryExecutor::isForecastFormula("  forecast(q, 'seasonal', 3)"));
    EXPECT_FALSE(DerivedQueryExecutor::isForecastFormula("a + b"));
    EXPECT_FALSE(DerivedQueryExecutor::isForecastFormula("anomalies(a, 'basic', 2)"));
}

// ===========================================================================
// Non-numeric sub-query fields are rejected, booleans included
//
// Canonical rule (CLAUDE.md "Non-Numeric Fields in Queries"): booleans are
// non-numeric, exactly as strings are. A formula is arithmetic, so a
// non-numeric operand is an error rather than a coercion. Booleans used to be
// silently folded to 1.0/0.0 here while strings threw — so `a * 100` over a
// bool field computed a plausible number over a type the query path refuses to
// aggregate. Worse, with an aggregationInterval the sub-query has already been
// reduced to LATEST-per-bucket, so the formula ran on latest-per-bucket values
// rather than the every-point series its author expected.
// ===========================================================================
TEST_F(DerivedQueryExecutorSeastarTest, NonNumericSubQueryFieldThrows) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t intervalNs = 60000000000ULL;

        TimeStarInsert<bool> boolInsert("door", "open");
        boolInsert.addTag("id", "d1");
        for (size_t i = 0; i < 5; ++i) {
            boolInsert.addValue(startNs + i * intervalNs, i % 2 == 0);
        }
        shardedInsert(eng.eng, std::move(boolInsert));

        TimeStarInsert<std::string> strInsert("door", "label");
        strInsert.addTag("id", "d1");
        for (size_t i = 0; i < 5; ++i) {
            strInsert.addValue(startNs + i * intervalNs, "s" + std::to_string(i));
        }
        shardedInsert(eng.eng, std::move(strInsert));

        DerivedQueryExecutor executor(&eng.eng);

        // Booleans and strings must both be rejected — same rule, same error.
        for (const std::string& field : {"open", "label"}) {
            DerivedQueryRequest request;
            request.formula = "a * 100";
            request.startTime = startNs;
            request.endTime = startNs + 10 * intervalNs;

            QueryRequest qr;
            qr.measurement = "door";
            qr.fields = {field};
            qr.scopes = {{"id", "d1"}};
            qr.startTime = startNs;
            qr.endTime = startNs + 10 * intervalNs;
            request.queries["a"] = qr;

            EXPECT_THROW(
                {
                    try {
                        executor.execute(request).get();
                    } catch (const DerivedQueryException& e) {
                        EXPECT_NE(std::string(e.what()).find("non-numeric"), std::string::npos)
                            << "field=" << field << " message=" << e.what();
                        throw;
                    }
                },
                DerivedQueryException)
                << "REGRESSION: non-numeric field '" << field << "' was accepted into formula arithmetic";
        }

        // A numeric field on the same measurement still works.
        {
            std::vector<std::pair<uint64_t, double>> pts;
            for (size_t i = 0; i < 5; ++i) {
                pts.push_back({startNs + i * intervalNs, static_cast<double>(i)});
            }
            insertFloatSeries(eng.eng, "door", "angle", {{"id", "d1"}}, pts);

            DerivedQueryRequest request;
            request.formula = "a * 100";
            request.startTime = startNs;
            request.endTime = startNs + 10 * intervalNs;

            QueryRequest qr;
            qr.measurement = "door";
            qr.fields = {"angle"};
            qr.scopes = {{"id", "d1"}};
            qr.startTime = startNs;
            qr.endTime = startNs + 10 * intervalNs;
            request.queries["a"] = qr;

            auto result = executor.execute(request).get();
            ASSERT_EQ(result.values.size(), 5u);
            EXPECT_DOUBLE_EQ(result.values[4], 400.0);
        }
    })
        .join()
        .get();
}

// ===========================================================================
// 18d. A malformed aggregationInterval is a CLIENT error on /derived, as it
// already is on /query.
//
// Bug: HttpQueryHandler::parseInterval signals "1x" / " 1d" / "-1d" / "abc"
// with QueryParseException, which is not a DerivedQueryException -- so it
// escaped executeFromJson()/executeFromJsonWithAnomaly() past the /derived
// handler's 400 branch into its catch-all, and the endpoint answered
// HTTP 500 INTERNAL_ERROR ("Internal server error", with the actual reason
// swallowed) where /query answers 400 for the identical literal.  Both entry
// points must now raise DerivedQueryException, which the handler renders as
// 400 on the JSON and the protobuf path alike.
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, MalformedAggregationIntervalIsAClientError) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t endNs = startNs + 3600000000000ULL;

        // Every one of these is rejected with 400 by POST /query.
        const char* malformed[] = {R"("1x")", R"(" 1d")", R"("-1d")", R"("abc")", R"("d")", R"("1 d")"};

        for (const char* literal : malformed) {
            DerivedQueryExecutor executor(&eng.eng);

            // Entry point 1: arithmetic formula.
            EXPECT_THROW(
                {
                    try {
                        executor.executeFromJson(derivedJsonBody("a * 1", startNs, endNs, literal)).get();
                    } catch (const DerivedQueryException& e) {
                        EXPECT_NE(std::string(e.what()).find("aggregationInterval"), std::string::npos)
                            << "message should name the offending parameter: " << e.what();
                        throw;
                    }
                },
                DerivedQueryException)
                << "REGRESSION: interval " << literal << " did not raise a client error on executeFromJson";

            // Entry point 2: the forecast/anomaly path parses the interval in
            // its own function and needs the same treatment.
            EXPECT_THROW(
                executor
                    .executeFromJsonWithAnomaly(derivedJsonBody("forecast(a, 'linear', 2)", startNs, endNs, literal))
                    .get(),
                DerivedQueryException)
                << "REGRESSION: interval " << literal << " did not raise a client error on executeFromJsonWithAnomaly";
        }
    })
        .join()
        .get();
}

// ===========================================================================
// 18e. "aggregationInterval": null and 0 mean "no interval", exactly as on
// POST /query -- they are not errors.
//
// Before the field became optional they were rejected outright (a JSON null
// and a JSON number both failed to parse into a std::string), so a client that
// spelled "no bucketing" explicitly got HTTP 400.  Both spellings must now
// return the raw, unbucketed series.
// ===========================================================================

TEST_F(DerivedQueryExecutorSeastarTest, NullAndZeroAggregationIntervalMeanNoInterval) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kRawPoints = 30;

        std::vector<std::pair<uint64_t, double>> cpuPoints;
        for (size_t i = 0; i < kRawPoints; ++i) {
            cpuPoints.push_back({startNs + i * minuteNs, static_cast<double>(i)});
        }
        insertFloatSeries(eng.eng, "server.metrics", "cpu", {{"host", "h1"}}, cpuPoints);

        const uint64_t endNs = startNs + kRawPoints * minuteNs;

        // Reference: omitting the key entirely.
        DerivedQueryExecutor reference(&eng.eng);
        auto absent = reference.executeFromJson(derivedJsonBody("a * 1", startNs, endNs, nullptr)).get();
        ASSERT_EQ(absent.timestamps.size(), kRawPoints);

        // "null", "0" and "" must all agree with it.
        const char* noIntervalSpellings[] = {"null", "0", R"("")"};
        for (const char* literal : noIntervalSpellings) {
            DerivedQueryExecutor executor(&eng.eng);
            auto result = executor.executeFromJson(derivedJsonBody("a * 1", startNs, endNs, literal)).get();

            EXPECT_EQ(result.timestamps, absent.timestamps) << "spelling=" << literal;
            EXPECT_EQ(result.values, absent.values) << "spelling=" << literal;
        }

        // Same on the forecast entry point, which parses the interval
        // separately: the fit must see every raw point.
        for (const char* literal : noIntervalSpellings) {
            DerivedQueryExecutor executor(&eng.eng);
            auto variant =
                executor
                    .executeFromJsonWithAnomaly(derivedJsonBody("forecast(a, 'linear', 2)", startNs, endNs, literal))
                    .get();

            ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant)) << "spelling=" << literal;
            const auto& result = std::get<forecast::ForecastQueryResult>(variant);
            EXPECT_TRUE(result.success) << "spelling=" << literal;
            EXPECT_EQ(result.statistics.historicalPoints, kRawPoints) << "spelling=" << literal;
        }
    })
        .join()
        .get();
}

// ===========================================================================
// Phase 3 — forecast()/anomalies() fan out over EVERY series a leg resolves to
//
// Bug: /derived refused any leg resolving to more than one series ("Sub-query
// 'a' returned 2 series but derived queries require exactly one series"),
// which blocked the natural use of forecast()/anomalies(): run per series and
// return one result group per series.  Worse, a MULTI-FIELD leg was not even
// refused -- it silently forecast query.fields[0] and dropped the rest with no
// diagnostic at all.
//
// The response schema already modelled the answer (ForecastSeriesPiece /
// AnomalySeriesPiece carry group_tags, ForecastStatistics carries
// series_count) and both executors already accepted N series over a shared
// axis; only the adapter was missing.
// ===========================================================================

// A /derived body for one arbitrary leg, at an optional interval.
static std::string derivedJsonBodyForLeg(const std::string& leg, const std::string& formula, uint64_t startNs,
                                         uint64_t endNs, const char* intervalLiteral) {
    std::string json = R"json({"queries":{"a":")json" + leg + R"json("},"formula":")json" + formula +
                       R"json(","startTime":)json" + std::to_string(startNs) + R"json(,"endTime":)json" +
                       std::to_string(endNs);
    if (intervalLiteral != nullptr) {
        json += R"json(,"aggregationInterval":)json";
        json += intervalLiteral;
    }
    json += "}";
    return json;
}

// The distinct group_tags of a forecast/anomaly result, in first-appearance
// order (the four/five pieces of one group repeat its tags).
template <typename ResultT>
static std::vector<std::vector<std::string>> distinctGroupTags(const ResultT& result) {
    std::vector<std::vector<std::string>> seen;
    for (const auto& piece : result.series) {
        if (std::find(seen.begin(), seen.end(), piece.groupTags) == seen.end()) {
            seen.push_back(piece.groupTags);
        }
    }
    return seen;
}

// Seed motor.vibration: 2 devices x 3 fields, co-sampled every minute.
static void seedTwoDeviceMotor(seastar::sharded<Engine>& eng, uint64_t startNs, uint64_t minuteNs, size_t points) {
    const std::vector<std::string> fields = {"10hz", "20hz", "30hz"};
    const std::map<std::string, double> deviceBase = {{"DEV-A", 10.0}, {"DEV-B", 90.0}};
    for (const auto& [device, base] : deviceBase) {
        for (size_t f = 0; f < fields.size(); ++f) {
            std::vector<std::pair<uint64_t, double>> pts;
            for (size_t i = 0; i < points; ++i) {
                // A clean per-(device, field) ramp so every group has a
                // well-defined and DISTINCT linear fit.
                pts.push_back({startNs + i * minuteNs, base + static_cast<double>(f) * 100.0 + static_cast<double>(i)});
            }
            insertFloatSeries(eng, "motor.vibration", fields[f], {{"deviceId", device}}, pts);
        }
    }
}

// ---- case D: two devices, grouped -> two forecast groups (was HTTP 400) ----
TEST_F(DerivedQueryExecutorSeastarTest, ForecastFansOutOverGroupedDevices) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 60;
        seedTwoDeviceMotor(eng.eng, startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant =
            executor
                .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                    "avg:motor.vibration(10hz){} by {deviceId}", "forecast(a, 'linear', 2)", startNs, endNs, R"("5m")"))
                .get();

        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
        const auto& result = std::get<forecast::ForecastQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;

        EXPECT_EQ(result.statistics.seriesCount, 2u) << "REGRESSION: the leg was folded back to one series";
        auto groups = distinctGroupTags(result);
        ASSERT_EQ(groups.size(), 2u);
        EXPECT_EQ(groups[0], (std::vector<std::string>{"deviceId=DEV-A"}));
        EXPECT_EQ(groups[1], (std::vector<std::string>{"deviceId=DEV-B"}));

        // Four pieces per group (forecast, upper, lower, and the historical
        // series) -- the per-group piece count is unchanged, there are simply
        // two groups of them now.
        EXPECT_EQ(result.series.size() % 2u, 0u);
        EXPECT_GT(result.series.size(), 4u);

        // Every piece spans the ONE shared axis the result carries.
        for (const auto& piece : result.series) {
            EXPECT_EQ(piece.values.size(), result.times.size()) << "piece=" << piece.piece;
        }
    })
        .join()
        .get();
}

TEST_F(DerivedQueryExecutorSeastarTest, AnomaliesFanOutOverGroupedDevices) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 60;
        seedTwoDeviceMotor(eng.eng, startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant =
            executor
                .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                    "avg:motor.vibration(10hz){} by {deviceId}", "anomalies(a, 'basic', 2)", startNs, endNs, R"("5m")"))
                .get();

        ASSERT_TRUE(std::holds_alternative<anomaly::AnomalyQueryResult>(variant));
        const auto& result = std::get<anomaly::AnomalyQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;

        auto groups = distinctGroupTags(result);
        ASSERT_EQ(groups.size(), 2u) << "REGRESSION: the leg was folded back to one series";
        EXPECT_EQ(groups[0], (std::vector<std::string>{"deviceId=DEV-A"}));
        EXPECT_EQ(groups[1], (std::vector<std::string>{"deviceId=DEV-B"}));

        // AnomalyStatistics has no seriesCount field, so the group count shows
        // up in totalPoints: both groups are scanned, not just one.
        EXPECT_EQ(result.statistics.totalPoints, 2u * result.times.size());
        for (const auto& piece : result.series) {
            EXPECT_EQ(piece.values.size(), result.times.size()) << "piece=" << piece.piece;
        }
    })
        .join()
        .get();
}

// ---- case B: a multi-field leg -> one group per field, labelled ------------
TEST_F(DerivedQueryExecutorSeastarTest, ForecastFansOutOverRequestedFieldsWithFieldLabel) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 60;
        seedTwoDeviceMotor(eng.eng, startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant = executor
                           .executeFromJsonWithAnomaly(
                               derivedJsonBodyForLeg("avg:motor.vibration(10hz,20hz,30hz){deviceId:DEV-A}",
                                                     "forecast(a, 'linear', 2)", startNs, endNs, R"("5m")"))
                           .get();

        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
        const auto& result = std::get<forecast::ForecastQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;

        EXPECT_EQ(result.statistics.seriesCount, 3u)
            << "REGRESSION: two of the three requested fields were silently dropped";
        auto groups = distinctGroupTags(result);
        ASSERT_EQ(groups.size(), 3u);
        // A SCOPE is a filter, not a grouping key: /query attaches tags to a
        // series only under a `by` clause, so a scoped leg's groups carry no
        // tags at all -- exactly as they did before fan-out.  What IS new is
        // the "_field=" label, which is appended after any tags there are.
        EXPECT_EQ(groups[0], (std::vector<std::string>{"_field=10hz"}));
        EXPECT_EQ(groups[1], (std::vector<std::string>{"_field=20hz"}));
        EXPECT_EQ(groups[2], (std::vector<std::string>{"_field=30hz"}));
    })
        .join()
        .get();
}

// ...and a SINGLE-field leg carries no field label at all, so its group_tags
// are byte-identical to the pre-fan-out response.
TEST_F(DerivedQueryExecutorSeastarTest, SingleFieldLegGroupTagsCarryNoFieldLabel) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 60;
        seedTwoDeviceMotor(eng.eng, startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant =
            executor
                .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                    "avg:motor.vibration(10hz){deviceId:DEV-A}", "forecast(a, 'linear', 2)", startNs, endNs, R"("5m")"))
                .get();

        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
        const auto& result = std::get<forecast::ForecastQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;

        EXPECT_EQ(result.statistics.seriesCount, 1u);
        auto groups = distinctGroupTags(result);
        ASSERT_EQ(groups.size(), 1u);
        // Empty, because a scope is a filter rather than a grouping key -- and
        // it must STAY empty: a single-field leg gains no synthetic _field=
        // entry, so this response is byte-identical to the pre-fan-out one.
        EXPECT_TRUE(groups[0].empty()) << "a single-field leg must not gain a synthetic _field= entry";
    })
        .join()
        .get();
}

// A GROUPED multi-field leg carries both: the grouping tags in tag-key order,
// then the field label last.  This is the case that pins where "_field=" sorts.
TEST_F(DerivedQueryExecutorSeastarTest, GroupedMultiFieldLegLabelsTagsThenField) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 60;
        seedTwoDeviceMotor(eng.eng, startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant = executor
                           .executeFromJsonWithAnomaly(
                               derivedJsonBodyForLeg("avg:motor.vibration(10hz,20hz){} by {deviceId}",
                                                     "forecast(a, \'linear\', 2)", startNs, endNs, R"("5m")"))
                           .get();

        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
        const auto& result = std::get<forecast::ForecastQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;

        EXPECT_EQ(result.statistics.seriesCount, 4u) << "2 devices x 2 fields";
        auto groups = distinctGroupTags(result);
        ASSERT_EQ(groups.size(), 4u);
        // Tag-major (a device's fields stay contiguous), tags first, field last.
        EXPECT_EQ(groups[0], (std::vector<std::string>{"deviceId=DEV-A", "_field=10hz"}));
        EXPECT_EQ(groups[1], (std::vector<std::string>{"deviceId=DEV-A", "_field=20hz"}));
        EXPECT_EQ(groups[2], (std::vector<std::string>{"deviceId=DEV-B", "_field=10hz"}));
        EXPECT_EQ(groups[3], (std::vector<std::string>{"deviceId=DEV-B", "_field=20hz"}));
    })
        .join()
        .get();
}

// ---- requirement 6: a single-series leg is BYTE-IDENTICAL ------------------
//
// The reference is built the way executeForecast() built it BEFORE fan-out:
// one series straight out of the query handler, its own timestamps as the
// axis, its tag map flattened to group tags, handed to executeMulti() in a
// one-element vector.  Everything except the wall-clock timing must match.
TEST_F(DerivedQueryExecutorSeastarTest, SingleSeriesForecastMatchesThePreFanOutPathExactly) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        const uint64_t fiveMinNs = 300000000000ULL;
        constexpr size_t kPoints = 60;
        seedTwoDeviceMotor(eng.eng, startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        // ---- the answer /derived gives today ----
        DerivedQueryExecutor executor(&eng.eng);
        auto variant =
            executor
                .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                    "avg:motor.vibration(10hz){deviceId:DEV-A}", "forecast(a, 'linear', 2)", startNs, endNs, R"("5m")"))
                .get();
        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
        const auto& got = std::get<forecast::ForecastQueryResult>(variant);
        ASSERT_TRUE(got.success) << got.errorMessage;

        // ---- the answer the pre-fan-out code gave ----
        http::HttpQueryHandler handler(&eng.eng);
        QueryRequest plain;
        plain.aggregation = AggregationMethod::AVG;
        plain.measurement = "motor.vibration";
        plain.fields = {"10hz"};
        plain.scopes = {{"deviceId", "DEV-A"}};
        plain.startTime = startNs;
        plain.endTime = endNs;
        plain.aggregationInterval = fiveMinNs;
        auto response = handler.executeQuery(plain).get();
        ASSERT_TRUE(response.success);
        ASSERT_EQ(response.series.size(), 1u);

        const auto& fieldData = response.series[0].fields.at("10hz");
        std::vector<uint64_t> refTimes = fieldData.first;
        std::vector<double> refValues = std::get<std::vector<double>>(fieldData.second);

        std::vector<std::string> refGroupTags;
        for (const auto& [key, value] : response.series[0].tags) {
            refGroupTags.push_back(key + "=" + value);
        }

        forecast::ForecastConfig config;
        config.algorithm = forecast::Algorithm::LINEAR;
        config.deviations = 2.0;
        // The horizon math the old code ran on the single series' timestamps.
        uint64_t duration = endNs - startNs;
        if (refTimes.size() >= 2) {
            uint64_t span = refTimes.back() - refTimes.front();
            uint64_t interval = span / (refTimes.size() - 1);
            if (interval > 0) {
                config.forecastHorizon = duration / interval;
            }
        }

        forecast::ForecastExecutor reference;
        std::vector<std::vector<double>> refSeriesValues = {refValues};
        std::vector<std::vector<std::string>> refSeriesGroupTags = {refGroupTags};
        auto want = reference.executeMulti(refTimes, refSeriesValues, refSeriesGroupTags, config);
        ASSERT_TRUE(want.success) << want.errorMessage;

        // ---- and they must agree on everything but the clock ----
        EXPECT_EQ(got.times, want.times);
        EXPECT_EQ(got.forecastStartIndex, want.forecastStartIndex);
        ASSERT_EQ(got.series.size(), want.series.size());
        for (size_t i = 0; i < got.series.size(); ++i) {
            EXPECT_EQ(got.series[i].piece, want.series[i].piece) << "piece " << i;
            EXPECT_EQ(got.series[i].groupTags, want.series[i].groupTags) << "piece " << i;
            ASSERT_EQ(got.series[i].values.size(), want.series[i].values.size()) << "piece " << i;
            for (size_t j = 0; j < got.series[i].values.size(); ++j) {
                const auto& g = got.series[i].values[j];
                const auto& w = want.series[i].values[j];
                ASSERT_EQ(g.has_value(), w.has_value()) << "piece " << i << " value " << j;
                if (g.has_value()) {
                    // NaN is a legitimate value here and NaN != NaN.
                    if (std::isnan(*w)) {
                        EXPECT_TRUE(std::isnan(*g)) << "piece " << i << " value " << j;
                    } else {
                        EXPECT_DOUBLE_EQ(*g, *w) << "piece " << i << " value " << j;
                    }
                }
            }
        }
        EXPECT_EQ(got.statistics.algorithm, want.statistics.algorithm);
        EXPECT_DOUBLE_EQ(got.statistics.deviations, want.statistics.deviations);
        EXPECT_EQ(got.statistics.seasonality, want.statistics.seasonality);
        EXPECT_DOUBLE_EQ(got.statistics.slope, want.statistics.slope);
        EXPECT_DOUBLE_EQ(got.statistics.intercept, want.statistics.intercept);
        EXPECT_DOUBLE_EQ(got.statistics.rSquared, want.statistics.rSquared);
        EXPECT_DOUBLE_EQ(got.statistics.residualStdDev, want.statistics.residualStdDev);
        EXPECT_EQ(got.statistics.historicalPoints, want.statistics.historicalPoints);
        EXPECT_EQ(got.statistics.forecastPoints, want.statistics.forecastPoints);
        EXPECT_EQ(got.statistics.seriesCount, want.statistics.seriesCount);
        EXPECT_EQ(got.statistics.originalPoints, want.statistics.originalPoints);
        EXPECT_EQ(got.statistics.windowedPoints, want.statistics.windowedPoints);
    })
        .join()
        .get();
}

// Same guard for anomalies(), whose reference is AnomalyExecutor::execute() --
// the single-series entry point the pre-fan-out code called.
TEST_F(DerivedQueryExecutorSeastarTest, SingleSeriesAnomaliesMatchThePreFanOutPathExactly) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        const uint64_t fiveMinNs = 300000000000ULL;
        constexpr size_t kPoints = 60;
        seedTwoDeviceMotor(eng.eng, startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant =
            executor
                .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                    "avg:motor.vibration(10hz){deviceId:DEV-A}", "anomalies(a, 'basic', 2)", startNs, endNs, R"("5m")"))
                .get();
        ASSERT_TRUE(std::holds_alternative<anomaly::AnomalyQueryResult>(variant));
        const auto& got = std::get<anomaly::AnomalyQueryResult>(variant);
        ASSERT_TRUE(got.success) << got.errorMessage;

        http::HttpQueryHandler handler(&eng.eng);
        QueryRequest plain;
        plain.aggregation = AggregationMethod::AVG;
        plain.measurement = "motor.vibration";
        plain.fields = {"10hz"};
        plain.scopes = {{"deviceId", "DEV-A"}};
        plain.startTime = startNs;
        plain.endTime = endNs;
        plain.aggregationInterval = fiveMinNs;
        auto response = handler.executeQuery(plain).get();
        ASSERT_TRUE(response.success);
        ASSERT_EQ(response.series.size(), 1u);

        const auto& fieldData = response.series[0].fields.at("10hz");
        std::vector<uint64_t> refTimes = fieldData.first;
        std::vector<double> refValues = std::get<std::vector<double>>(fieldData.second);
        std::vector<std::string> refGroupTags;
        for (const auto& [key, value] : response.series[0].tags) {
            refGroupTags.push_back(key + "=" + value);
        }

        anomaly::AnomalyConfig config;
        config.algorithm = anomaly::parseAlgorithm("basic");
        config.bounds = 2.0;

        anomaly::AnomalyExecutor reference;
        auto want = reference.execute(refTimes, refValues, refGroupTags, config);
        ASSERT_TRUE(want.success) << want.errorMessage;

        EXPECT_EQ(got.times, want.times);
        ASSERT_EQ(got.series.size(), want.series.size());
        for (size_t i = 0; i < got.series.size(); ++i) {
            EXPECT_EQ(got.series[i].piece, want.series[i].piece) << "piece " << i;
            EXPECT_EQ(got.series[i].groupTags, want.series[i].groupTags) << "piece " << i;
            ASSERT_EQ(got.series[i].alertValue.has_value(), want.series[i].alertValue.has_value()) << "piece " << i;
            if (got.series[i].alertValue.has_value()) {
                EXPECT_DOUBLE_EQ(*got.series[i].alertValue, *want.series[i].alertValue) << "piece " << i;
            }
            ASSERT_EQ(got.series[i].values.size(), want.series[i].values.size()) << "piece " << i;
            for (size_t j = 0; j < got.series[i].values.size(); ++j) {
                if (std::isnan(want.series[i].values[j])) {
                    EXPECT_TRUE(std::isnan(got.series[i].values[j])) << "piece " << i << " value " << j;
                } else {
                    EXPECT_DOUBLE_EQ(got.series[i].values[j], want.series[i].values[j])
                        << "piece " << i << " value " << j;
                }
            }
        }
        EXPECT_EQ(got.statistics.algorithm, want.statistics.algorithm);
        EXPECT_DOUBLE_EQ(got.statistics.bounds, want.statistics.bounds);
        EXPECT_EQ(got.statistics.seasonality, want.statistics.seasonality);
        EXPECT_EQ(got.statistics.anomalyCount, want.statistics.anomalyCount);
        EXPECT_EQ(got.statistics.totalPoints, want.statistics.totalPoints);
    })
        .join()
        .get();
}

// ---- case C: an ungrouped multi-device leg is still ONE series ------------
//
// `{}` with no `by` clause is a cross-series merge at the /query layer -- one
// series, aggregated across devices at equal timestamps.  That is canonical
// /query semantics (CLAUDE.md, "Aggregation Result Shape"), not something the
// fan-out may override.
TEST_F(DerivedQueryExecutorSeastarTest, UngroupedMultiDeviceLegStaysOneForecastGroup) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 60;
        seedTwoDeviceMotor(eng.eng, startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant = executor
                           .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                               "avg:motor.vibration(10hz){}", "forecast(a, 'linear', 2)", startNs, endNs, R"("5m")"))
                           .get();

        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
        const auto& result = std::get<forecast::ForecastQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;
        EXPECT_EQ(result.statistics.seriesCount, 1u) << "the cross-series merge must not be split by the fan-out";
    })
        .join()
        .get();
}

// ---- the ragged case: groups that share no timestamps at all --------------
//
// Two devices sampled on DISJOINT offsets (one on the minute, one at :30) have
// no timestamp in common, so the shared axis is the union of both and each
// group carries NaN at the other's samples.  NaN is missing (docs/nan_policy.md)
// and the fit must simply not see those slots -- each device must recover its
// OWN trend, not a blend of the two.
TEST_F(DerivedQueryExecutorSeastarTest, ForecastOverGroupsWithDisjointTimestampsUsesTheUnionAxis) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        const uint64_t halfMinuteNs = 30000000000ULL;
        constexpr size_t kPoints = 40;

        std::vector<std::pair<uint64_t, double>> onTheMinute;
        std::vector<std::pair<uint64_t, double>> offBeat;
        for (size_t i = 0; i < kPoints; ++i) {
            onTheMinute.push_back({startNs + i * minuteNs, 10.0 + static_cast<double>(i)});
            offBeat.push_back({startNs + i * minuteNs + halfMinuteNs, 500.0 + 3.0 * static_cast<double>(i)});
        }
        insertFloatSeries(eng.eng, "ragged", "v", {{"deviceId", "DEV-A"}}, onTheMinute);
        insertFloatSeries(eng.eng, "ragged", "v", {{"deviceId", "DEV-B"}}, offBeat);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant = executor
                           .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                               "avg:ragged(v){} by {deviceId}", "forecast(a, \'linear\', 2)", startNs, endNs, nullptr))
                           .get();

        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
        const auto& result = std::get<forecast::ForecastQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;

        // The axis is the UNION: every sample of both devices, none merged.
        EXPECT_EQ(result.forecastStartIndex, 2u * kPoints);
        EXPECT_EQ(result.statistics.seriesCount, 2u);

        // Each group is fitted on its own points only, so the projections stay
        // an order of magnitude apart -- a fit poisoned by the NaN gaps would
        // give NaN, and one blended across devices would give a single trend.
        auto groups = distinctGroupTags(result);
        ASSERT_EQ(groups.size(), 2u);

        for (const auto& piece : result.series) {
            if (piece.piece != "forecast") {
                continue;
            }
            ASSERT_FALSE(piece.values.empty());
            const auto& last = piece.values.back();
            ASSERT_TRUE(last.has_value()) << "group " << piece.groupTags.at(0);
            ASSERT_FALSE(std::isnan(*last)) << "REGRESSION: NaN gaps poisoned the fit for " << piece.groupTags.at(0);
            if (piece.groupTags.at(0) == "deviceId=DEV-A") {
                EXPECT_GT(*last, 10.0);
                EXPECT_LT(*last, 200.0) << "DEV-A must not be blended with DEV-B";
            } else {
                EXPECT_GT(*last, 500.0) << "DEV-B must keep its own, much larger trend";
            }
        }
    })
        .join()
        .get();
}

// ---- requirement 7: the ARITHMETIC formula path is unchanged (Phase 4) ----
TEST_F(DerivedQueryExecutorSeastarTest, ArithmeticFormulaStillRefusesAMultiSeriesLeg) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 20;
        seedTwoDeviceMotor(eng.eng, startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        try {
            executor
                .executeFromJson(derivedJsonBodyForLeg("avg:motor.vibration(10hz){} by {deviceId}", "a * 1", startNs,
                                                       endNs, R"("5m")"))
                .get();
            FAIL() << "Expected DerivedQueryException: arithmetic formulas are Phase 4";
        } catch (const DerivedQueryException& e) {
            std::string msg = e.what();
            EXPECT_NE(msg.find("exactly one series"), std::string::npos) << msg;
        }
    })
        .join()
        .get();
}

// ---- requirement 5: the per-leg cap is now reachable, as a CLIENT error ----
//
// DerivedQueryException is what the /derived handler turns into HTTP 400; an
// escaped std::exception would fall through to its catch-all and answer 500
// with the reason swallowed.
TEST_F(DerivedQueryExecutorSeastarTest, PerLegSeriesCapIsAClientError) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kDevices = 8;
        // Above the default minDataPoints of 10, so the "under the cap it
        // succeeds" branch below really does forecast all 8 groups.  At 5
        // points every group was declined for want of data and series_count
        // was 8 only because it reported the INPUT count -- the assertion
        // passed while the response carried no groups at all.
        constexpr size_t kPoints = 12;

        for (size_t d = 0; d < kDevices; ++d) {
            std::vector<std::pair<uint64_t, double>> pts;
            for (size_t i = 0; i < kPoints; ++i) {
                pts.push_back({startNs + i * minuteNs, static_cast<double>(d * 10 + i)});
            }
            insertFloatSeries(eng.eng, "fleet", "temp", {{"deviceId", "d" + std::to_string(d)}}, pts);
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryConfig capped;
        capped.maxSeriesPerLeg = 4;
        DerivedQueryExecutor executor(&eng.eng, capped);

        try {
            executor
                .executeFromJsonWithAnomaly(derivedJsonBodyForLeg("avg:fleet(temp){} by {deviceId}",
                                                                  "forecast(a, 'linear', 2)", startNs, endNs, nullptr))
                .get();
            FAIL() << "Expected DerivedQueryException from the per-leg cap";
        } catch (const DerivedQueryException& e) {
            std::string msg = e.what();
            EXPECT_NE(msg.find("per-leg limit"), std::string::npos) << msg;
            EXPECT_NE(msg.find("4"), std::string::npos) << "must state the cap: " << msg;
            EXPECT_NE(msg.find("scope"), std::string::npos) << "must be actionable: " << msg;
        }

        // Under the cap the same leg succeeds.
        DerivedQueryConfig roomy;
        roomy.maxSeriesPerLeg = 50;
        DerivedQueryExecutor ok(&eng.eng, roomy);
        auto variant =
            ok.executeFromJsonWithAnomaly(derivedJsonBodyForLeg("avg:fleet(temp){} by {deviceId}",
                                                                "forecast(a, 'linear', 2)", startNs, endNs, nullptr))
                .get();
        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
        const auto& uncapped = std::get<forecast::ForecastQueryResult>(variant);
        EXPECT_EQ(uncapped.statistics.seriesCount, kDevices);
        EXPECT_EQ(uncapped.statistics.declinedSeriesCount, 0u)
            << "every group must really be forecast, or seriesCount pins nothing";
        EXPECT_EQ(distinctGroupTags(uncapped).size(), kDevices);
    })
        .join()
        .get();
}

// ===========================================================================
// D-H: every group's VALUES must be paired with its own LABEL.
//
// The fan-out tests above assert group counts, group_tags and piece widths.
// All of them would still pass if row i were emitted against group j's tags --
// the labels would be right, the numbers would belong to the wrong device, and
// nothing in the suite would notice.  Under a fleet-wide per-device forecast
// that is the worst possible failure: a plausible answer attributed to the
// wrong machine.
//
// These tests give each group an ANALYTICALLY DISTINCT trend (rising /
// falling / flat, distinct bases) and check every value against a closed form
// evaluated at the RESULT'S OWN timestamps -- so the assertions survive
// auto-windowing, and a mis-pairing changes the sign of the projection rather
// than a digit somewhere.
// ===========================================================================

// The closed form each seeded group follows: value(ts) = base + slope*minutes.
struct SeededTrend {
    std::string device;
    double base;
    double slopePerMinute;
};

static double seededValueAt(const SeededTrend& trend, uint64_t ts, uint64_t startNs, uint64_t minuteNs) {
    const double minutes = static_cast<double>(ts - startNs) / static_cast<double>(minuteNs);
    return trend.base + trend.slopePerMinute * minutes;
}

// Rising, falling and flat, with three widely separated bases: no two groups
// agree on a value at ANY timestamp, historical or projected.
static const std::vector<SeededTrend>& distinctTrends() {
    static const std::vector<SeededTrend> trends = {
        {"DEV-A", 1000.0, 10.0},   // rising
        {"DEV-B", 5000.0, -20.0},  // falling
        {"DEV-C", 3000.0, 0.0},    // flat
    };
    return trends;
}

static void seedDistinctTrends(seastar::sharded<Engine>& eng, const std::string& measurement, const std::string& field,
                               uint64_t startNs, uint64_t minuteNs, size_t points, double fieldOffset = 0.0) {
    for (const auto& trend : distinctTrends()) {
        std::vector<std::pair<uint64_t, double>> pts;
        pts.reserve(points);
        for (size_t i = 0; i < points; ++i) {
            const uint64_t ts = startNs + i * minuteNs;
            pts.push_back({ts, seededValueAt(trend, ts, startNs, minuteNs) + fieldOffset});
        }
        insertFloatSeries(eng, measurement, field, {{"deviceId", trend.device}}, pts);
    }
}

// ---- forecast: past AND projection belong to the labelled device -----------
TEST_F(DerivedQueryExecutorSeastarTest, ForecastGroupValuesBelongToTheGroupTheyAreLabelledWith) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 60;
        seedDistinctTrends(eng.eng, "pairing.motor", "v", startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant =
            executor
                .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                    "avg:pairing.motor(v){} by {deviceId}", "forecast(a, 'linear', 2)", startNs, endNs, R"("1m")"))
                .get();

        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
        const auto& result = std::get<forecast::ForecastQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;
        ASSERT_EQ(distinctGroupTags(result).size(), distinctTrends().size());

        const size_t fsi = result.forecastStartIndex;
        ASSERT_GT(fsi, 0u);
        ASSERT_LT(fsi, result.times.size());

        for (const auto& trend : distinctTrends()) {
            const std::vector<std::string> label{"deviceId=" + trend.device};

            const auto* past = result.getPiece("past", label);
            ASSERT_NE(past, nullptr) << "no past piece for " << trend.device;
            ASSERT_EQ(past->values.size(), result.times.size());

            // Historical region: this group's OWN stored values, at this
            // group's own timestamps.
            for (size_t i = 0; i < fsi; ++i) {
                ASSERT_TRUE(past->values[i].has_value()) << trend.device << " past[" << i << "] is null";
                const double want = seededValueAt(trend, result.times[i], startNs, minuteNs);
                EXPECT_NEAR(*past->values[i], want, 1e-6)
                    << trend.device << " past[" << i << "]: values are paired with the WRONG group's label";
            }
            // ...and nothing past the split.
            for (size_t i = fsi; i < past->values.size(); ++i) {
                EXPECT_FALSE(past->values[i].has_value()) << trend.device << " past[" << i << "]";
            }

            // Projection: the data is exactly linear, so the fit is exact and
            // the projection is the same closed form continued forward.  A
            // swapped row shows up as the wrong SIGN here (DEV-A rises, DEV-B
            // falls, DEV-C is flat), not as a rounding difference.
            const auto* projected = result.getPiece("forecast", label);
            ASSERT_NE(projected, nullptr) << "no forecast piece for " << trend.device;
            for (size_t i = fsi; i < projected->values.size(); ++i) {
                ASSERT_TRUE(projected->values[i].has_value()) << trend.device << " forecast[" << i << "] is null";
                const double want = seededValueAt(trend, result.times[i], startNs, minuteNs);
                EXPECT_NEAR(*projected->values[i], want, 1e-3)
                    << trend.device << " forecast[" << i << "]: projection belongs to another group";
            }
        }

        // Belt and braces, stated as a SIGN rather than a value: each group's
        // projection moves in its own seeded direction.  Swapping any two rows
        // flips at least one of these, and the flat group cannot absorb either
        // of the others.
        const size_t last = result.times.size() - 1;
        auto projection = [&](const std::string& device, size_t i) {
            const auto* p = result.getPiece("forecast", {"deviceId=" + device});
            return p->values[i].value();
        };
        EXPECT_GT(projection("DEV-A", last), projection("DEV-A", fsi)) << "DEV-A was seeded rising";
        EXPECT_LT(projection("DEV-B", last), projection("DEV-B", fsi)) << "DEV-B was seeded falling";
        EXPECT_NEAR(projection("DEV-C", last), projection("DEV-C", fsi), 1e-6) << "DEV-C was seeded flat";
    })
        .join()
        .get();
}

// ---- forecast: (device x field) rows keep their compound label -------------
TEST_F(DerivedQueryExecutorSeastarTest, ForecastMultiFieldGroupValuesMatchTheirDeviceAndFieldLabel) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 60;
        constexpr double kOffsetLow = 0.0;
        constexpr double kOffsetHigh = 100000.0;  // far enough apart that no fit can be confused for the other
        seedDistinctTrends(eng.eng, "pairing.multi", "lo", startNs, minuteNs, kPoints, kOffsetLow);
        seedDistinctTrends(eng.eng, "pairing.multi", "hi", startNs, minuteNs, kPoints, kOffsetHigh);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant =
            executor
                .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                    "avg:pairing.multi(lo,hi){} by {deviceId}", "forecast(a, 'linear', 2)", startNs, endNs, R"("1m")"))
                .get();

        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
        const auto& result = std::get<forecast::ForecastQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;
        EXPECT_EQ(result.statistics.seriesCount, distinctTrends().size() * 2u);

        const size_t fsi = result.forecastStartIndex;
        ASSERT_GT(fsi, 0u);

        for (const auto& trend : distinctTrends()) {
            for (const auto& [field, offset] :
                 std::vector<std::pair<std::string, double>>{{"lo", kOffsetLow}, {"hi", kOffsetHigh}}) {
                const std::vector<std::string> label{"deviceId=" + trend.device, "_field=" + field};
                const auto* past = result.getPiece("past", label);
                ASSERT_NE(past, nullptr) << "no past piece for " << trend.device << "/" << field;

                for (size_t i = 0; i < fsi; ++i) {
                    ASSERT_TRUE(past->values[i].has_value());
                    const double want = seededValueAt(trend, result.times[i], startNs, minuteNs) + offset;
                    EXPECT_NEAR(*past->values[i], want, 1e-6)
                        << trend.device << "/" << field << " past[" << i << "]: wrong (device, field) row";
                }
            }
        }
    })
        .join()
        .get();
}

// ---- anomalies: the raw piece is the labelled device's own series ----------
TEST_F(DerivedQueryExecutorSeastarTest, AnomalyGroupValuesBelongToTheGroupTheyAreLabelledWith) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 60;
        seedDistinctTrends(eng.eng, "pairing.anom", "v", startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant =
            executor
                .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                    "avg:pairing.anom(v){} by {deviceId}", "anomalies(a, 'basic', 2)", startNs, endNs, R"("1m")"))
                .get();

        ASSERT_TRUE(std::holds_alternative<anomaly::AnomalyQueryResult>(variant));
        const auto& result = std::get<anomaly::AnomalyQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;
        ASSERT_EQ(distinctGroupTags(result).size(), distinctTrends().size());

        for (const auto& trend : distinctTrends()) {
            const std::vector<std::string> label{"deviceId=" + trend.device};
            const anomaly::AnomalySeriesPiece* raw = nullptr;
            for (const auto& piece : result.series) {
                if (piece.piece == "raw" && piece.groupTags == label) {
                    raw = &piece;
                    break;
                }
            }
            ASSERT_NE(raw, nullptr) << "no raw piece for " << trend.device;
            ASSERT_EQ(raw->values.size(), result.times.size());

            for (size_t i = 0; i < raw->values.size(); ++i) {
                const double want = seededValueAt(trend, result.times[i], startNs, minuteNs);
                EXPECT_NEAR(raw->values[i], want, 1e-6)
                    << trend.device << " raw[" << i << "]: values are paired with the WRONG group's label";
            }
        }
    })
        .join()
        .get();
}

// ===========================================================================
// D-B end to end: a group with no data in the retained window is OMITTED, not
// answered with a fabricated zero.
//
// The reporter's shape: three devices, one of which stops reporting partway
// through, with a history= window that excludes everything it ever sent.  The
// silent device used to come back as a constant 0.0 forecast with a zero-width
// band, labelled with its own deviceId, inside a "status":"success" response.
// ===========================================================================
TEST_F(DerivedQueryExecutorSeastarTest, ForecastOmitsAGroupWithNoDataInTheHistoryWindow) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t hourNs = 3600000000000ULL;
        constexpr size_t kHours = 240;  // 10 days

        // DEV-A and DEV-B report throughout; DEV-C stops after day 2.
        for (size_t i = 0; i < kHours; ++i) {
            const uint64_t ts = startNs + i * hourNs;
            insertFloatSeries(eng.eng, "stale.fleet", "f1", {{"deviceId", "DEV-A"}},
                              {{ts, 100.0 + static_cast<double>(i)}});
            insertFloatSeries(eng.eng, "stale.fleet", "f1", {{"deviceId", "DEV-B"}},
                              {{ts, 900.0 - static_cast<double>(i)}});
            if (i < 48) {
                insertFloatSeries(eng.eng, "stale.fleet", "f1", {{"deviceId", "DEV-C"}},
                                  {{ts, 500.0 + static_cast<double>(i)}});
            }
        }
        const uint64_t endNs = startNs + kHours * hourNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant = executor
                           .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                               "avg:stale.fleet(f1){deviceId:DEV-*} by {deviceId}",
                               "forecast(a, 'linear', 2, history='2d')", startNs, endNs, R"("1h")"))
                           .get();

        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
        const auto& result = std::get<forecast::ForecastQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;

        auto groups = distinctGroupTags(result);
        for (const auto& tags : groups) {
            ASSERT_FALSE(tags.empty());
            EXPECT_NE(tags[0], "deviceId=DEV-C")
                << "REGRESSION: a group with no data in the retained window was answered rather than omitted";
        }
        EXPECT_EQ(groups.size(), 2u) << "the two reporting devices must still be forecast";

        // The leg resolved to three series and two were answered.  series_count
        // reports what was EMITTED (it used to report the input count, 3, so
        // the response asserted a group it had dropped), and the third shows up
        // in declined_series_count -- which is the only thing that lets a
        // client tell "DEV-C has too little data" from "DEV-C does not exist".
        EXPECT_EQ(result.statistics.seriesCount, 2u);
        EXPECT_EQ(result.statistics.declinedSeriesCount, 1u);
        EXPECT_EQ(result.statistics.seriesCount + result.statistics.declinedSeriesCount, 3u)
            << "emitted + declined must account for every series the leg resolved to";
    })
        .join()
        .get();
}

// ===========================================================================
// D-A end to end: the fan-out output bound is a CLIENT error, and the bucketed
// form of the same query still succeeds.
// ===========================================================================
TEST_F(DerivedQueryExecutorSeastarTest, FanOutOutputBoundIsAClientErrorAndBucketingRelievesIt) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        const uint64_t secondNs = 1000000000ULL;
        constexpr size_t kDevices = 6;
        constexpr size_t kPoints = 40;

        // STAGGERED offsets: no two devices share a timestamp, so the union
        // axis is kDevices * kPoints long and the dense matrix is the SQUARE
        // of what is stored -- the amplification this bound exists to stop.
        for (size_t d = 0; d < kDevices; ++d) {
            std::vector<std::pair<uint64_t, double>> pts;
            for (size_t i = 0; i < kPoints; ++i) {
                pts.push_back({startNs + i * minuteNs + d * secondNs, static_cast<double>(d * 100 + i)});
            }
            insertFloatSeries(eng.eng, "skew.fleet", "v", {{"deviceId", "d" + std::to_string(d)}}, pts);
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        // 6 groups x 240 union timestamps = 1440 cells.
        DerivedQueryConfig bounded;
        bounded.maxFanOutPoints = 1000;
        DerivedQueryExecutor executor(&eng.eng, bounded);

        try {
            executor
                .executeFromJsonWithAnomaly(derivedJsonBodyForLeg("avg:skew.fleet(v){} by {deviceId}",
                                                                  "forecast(a, 'linear', 2)", startNs, endNs, nullptr))
                .get();
            FAIL() << "Expected DerivedQueryException from the fan-out output bound";
        } catch (const DerivedQueryException& e) {
            const std::string msg = e.what();
            EXPECT_NE(msg.find("1000"), std::string::npos) << "must state the limit: " << msg;
            EXPECT_NE(msg.find("aggregationInterval"), std::string::npos) << "must be actionable: " << msg;
        }

        // The SAME query with an aggregationInterval succeeds: bucketing
        // collapses the staggered union onto one grid, so the cell count drops
        // from groups x (groups*points) to groups x buckets.
        auto variant =
            executor
                .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                    "avg:skew.fleet(v){} by {deviceId}", "forecast(a, 'linear', 2)", startNs, endNs, R"("1m")"))
                .get();
        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
        const auto& bucketed = std::get<forecast::ForecastQueryResult>(variant);
        ASSERT_TRUE(bucketed.success) << bucketed.errorMessage;
        EXPECT_EQ(bucketed.statistics.seriesCount, kDevices);
        EXPECT_LE(bucketed.times.size(), kPoints + 1u + bucketed.statistics.forecastPoints)
            << "bucketing must collapse the staggered axis";

        // A leg that resolves to ONE series is EXEMPT from the bound, however
        // far over it the axis alone is.  That is the pre-fan-out behaviour
        // this work must not change -- it is linear in N rather than
        // quadratic, and it was already unbounded before the fan-out existed.
        // Limit of 10 against a 40-point single-series axis: still succeeds.
        DerivedQueryConfig tiny;
        tiny.maxFanOutPoints = 10;
        DerivedQueryExecutor exempt(&eng.eng, tiny);
        auto singleVariant =
            exempt
                .executeFromJsonWithAnomaly(derivedJsonBodyForLeg("avg:skew.fleet(v){deviceId:d0}",
                                                                  "forecast(a, 'linear', 2)", startNs, endNs, nullptr))
                .get();
        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(singleVariant));
        const auto& singleSeries = std::get<forecast::ForecastQueryResult>(singleVariant);
        ASSERT_TRUE(singleSeries.success) << singleSeries.errorMessage;
        EXPECT_EQ(singleSeries.statistics.seriesCount, 1u);
        EXPECT_GT(singleSeries.times.size(), tiny.maxFanOutPoints)
            << "the exempt leg must really be over the limit, or this pins nothing";
    })
        .join()
        .get();
}

// ===========================================================================
// D-G: anomaly total_points counts REAL points, not NaN padding.
//
// Every group is projected onto one shared axis and carries NaN wherever it
// has no sample, so summing row WIDTHS counted another group's timestamps as
// this group's data.
// ===========================================================================
TEST_F(DerivedQueryExecutorSeastarTest, AnomalyTotalPointsCountsRealPointsNotNaNPadding) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        const uint64_t secondNs = 1000000000ULL;
        constexpr size_t kDevices = 3;
        constexpr size_t kPoints = 20;

        // Staggered again: the union axis is 60 slots, each group really holds
        // 20 points, so the honest total is 60 -- which happens to equal the
        // axis length here, so also assert the per-group arithmetic below.
        for (size_t d = 0; d < kDevices; ++d) {
            std::vector<std::pair<uint64_t, double>> pts;
            for (size_t i = 0; i < kPoints; ++i) {
                pts.push_back({startNs + i * minuteNs + d * secondNs, static_cast<double>(d * 100 + i)});
            }
            insertFloatSeries(eng.eng, "padcount.fleet", "v", {{"deviceId", "d" + std::to_string(d)}}, pts);
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant =
            executor
                .executeFromJsonWithAnomaly(derivedJsonBodyForLeg("avg:padcount.fleet(v){} by {deviceId}",
                                                                  "anomalies(a, 'basic', 2)", startNs, endNs, nullptr))
                .get();

        ASSERT_TRUE(std::holds_alternative<anomaly::AnomalyQueryResult>(variant));
        const auto& result = std::get<anomaly::AnomalyQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;

        ASSERT_EQ(result.times.size(), kDevices * kPoints) << "the union axis should be fully staggered";
        EXPECT_EQ(result.statistics.totalPoints, kDevices * kPoints)
            << "total_points must count real points, not the " << (kDevices * result.times.size())
            << " cells of the padded matrix";

        // ...and the padding really is there, so the two numbers genuinely differ.
        size_t nullValues = 0;
        for (const auto& piece : result.series) {
            if (piece.piece != "raw") {
                continue;
            }
            for (double v : piece.values) {
                if (std::isnan(v)) {
                    ++nullValues;
                }
            }
        }
        EXPECT_EQ(nullValues, kDevices * result.times.size() - kDevices * kPoints);
    })
        .join()
        .get();
}

// ===========================================================================
// Phase 3.6 / R1: a DECLINED group must be visible in the response, and must
// not be counted as one that was answered.
//
// Bug: `forecast(a,'linear',2)` over `avg:m(v){} by {dev}` where dev=FULL had
// 40 points and dev=SPARSE had 9, on a 40-slot one-minute axis, answered
//
//     HTTP 200  "status":"success"
//     statistics: { "series_count": 2, ... }
//     group_tags present in the body:  ["dev=FULL"]        <- ONE group
//
// The response asserted two series and returned one, and the caller had no way
// to distinguish "DEV-SPARSE has too little data" from "DEV-SPARSE does not
// exist".  Failing the whole query would be wrong -- declining to fabricate a
// number is not a failed read, so this is NOT CLAUDE.md's QUERY_INCOMPLETE
// case -- but the response has to carry the fact.
// ===========================================================================
TEST_F(DerivedQueryExecutorSeastarTest, DeclinedForecastGroupsAreCountedNotSilentlyDropped) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kAxis = 40;
        constexpr size_t kSparse = 9;  // < the default minDataPoints of 10

        std::vector<std::pair<uint64_t, double>> full;
        for (size_t i = 0; i < kAxis; ++i) {
            full.push_back({startNs + i * minuteNs, 100.0 + static_cast<double>(i)});
        }
        insertFloatSeries(eng.eng, "decl.fleet", "v", {{"dev", "FULL"}}, full);

        std::vector<std::pair<uint64_t, double>> sparse;
        for (size_t i = 0; i < kSparse; ++i) {
            sparse.push_back({startNs + i * minuteNs, 900.0 + 3.0 * static_cast<double>(i)});
        }
        insertFloatSeries(eng.eng, "decl.fleet", "v", {{"dev", "SPARSE"}}, sparse);

        const uint64_t endNs = startNs + kAxis * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant = executor
                           .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                               "avg:decl.fleet(v){} by {dev}", "forecast(a, 'linear', 2)", startNs, endNs, R"("1m")"))
                           .get();

        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
        const auto& result = std::get<forecast::ForecastQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;

        auto groups = distinctGroupTags(result);
        ASSERT_EQ(groups.size(), 1u) << "only the FULL device has enough data to forecast";
        EXPECT_EQ(groups[0], (std::vector<std::string>{"dev=FULL"}));

        EXPECT_EQ(result.statistics.seriesCount, 1u)
            << "REGRESSION: series_count claimed a group the response does not carry";
        EXPECT_EQ(result.statistics.declinedSeriesCount, 1u)
            << "REGRESSION: the declined group vanished without a trace";
        EXPECT_EQ(result.statistics.seriesCount, groups.size())
            << "series_count must equal the groups actually present in the body";

        // And the JSON says so too, in both directions: the count is emitted
        // when there IS one...
        const std::string json = executor.formatForecastResponse(result);
        EXPECT_NE(json.find("\"declined_series_count\":1"), std::string::npos) << json.substr(0, 400);
    })
        .join()
        .get();
}

// ...and is ABSENT from a response where nothing was declined.  The field is
// additive on purpose: adding a constant `"declined_series_count":0` to every
// forecast response would have changed the bytes of every single-series answer
// that ever worked, which is the one thing this campaign may not do.  Protobuf
// behaves the same way on its own (a 0 uint64 is not put on the wire), so both
// transports agree that absent means none.
TEST_F(DerivedQueryExecutorSeastarTest, ASingleGroupResponseDeclinesNothingAndSaysNothing) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 60;
        seedTwoDeviceMotor(eng.eng, startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant =
            executor
                .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                    "avg:motor.vibration(10hz){deviceId:DEV-A}", "forecast(a, 'linear', 2)", startNs, endNs, R"("5m")"))
                .get();

        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
        const auto& result = std::get<forecast::ForecastQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;

        EXPECT_EQ(result.statistics.seriesCount, 1u);
        EXPECT_EQ(result.statistics.declinedSeriesCount, 0u);

        const std::string json = executor.formatForecastResponse(result);
        EXPECT_EQ(json.find("declined_series_count"), std::string::npos)
            << "a response with nothing declined must be byte-identical to before the field existed";
        // The rest of the statistics block is unchanged and still present.
        EXPECT_NE(json.find("\"series_count\":1"), std::string::npos);

        // Same for anomalies.
        auto anomVariant =
            executor
                .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                    "avg:motor.vibration(10hz){deviceId:DEV-A}", "anomalies(a, 'basic', 2)", startNs, endNs, R"("5m")"))
                .get();
        ASSERT_TRUE(std::holds_alternative<anomaly::AnomalyQueryResult>(anomVariant));
        const auto& anom = std::get<anomaly::AnomalyQueryResult>(anomVariant);
        ASSERT_TRUE(anom.success) << anom.errorMessage;
        EXPECT_EQ(anom.statistics.declinedSeriesCount, 0u);
        EXPECT_EQ(executor.formatAnomalyResponse(anom).find("declined_series_count"), std::string::npos);
    })
        .join()
        .get();
}

// ===========================================================================
// Phase 3.6 / R3: the anomaly half declines a group with too few OBSERVATIONS,
// exactly as the forecast half does, and reports it the same way.
//
// Bug: `anomalies(a,'basic',2)` where dev=SPARSE had ONE real point on a
// 40-slot axis returned, for that device, a 30-slot confidence envelope, a
// prediction at every timestamp and a score at every timestamp, HTTP 200.  The
// detectors' warm-up is indexed by SLOT, not by observation, so a group that is
// nearly all NaN leaves warm-up on the strength of a single sample.  Phase 3's
// fan-out is what made this reachable per device; before it, the two halves of
// /derived could not disagree because only one series ever arrived.
// ===========================================================================
TEST_F(DerivedQueryExecutorSeastarTest, DeclinedAnomalyGroupsAreCountedNotSilentlyDropped) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kAxis = 40;

        std::vector<std::pair<uint64_t, double>> full;
        for (size_t i = 0; i < kAxis; ++i) {
            full.push_back({startNs + i * minuteNs, 100.0 + static_cast<double>(i)});
        }
        insertFloatSeries(eng.eng, "adecl.fleet", "v", {{"dev", "FULL"}}, full);
        // ONE observation for the whole window.
        insertFloatSeries(eng.eng, "adecl.fleet", "v", {{"dev", "SPARSE"}}, {{startNs + 3 * minuteNs, 900.0}});

        const uint64_t endNs = startNs + kAxis * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant = executor
                           .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                               "avg:adecl.fleet(v){} by {dev}", "anomalies(a, 'basic', 2)", startNs, endNs, R"("1m")"))
                           .get();

        ASSERT_TRUE(std::holds_alternative<anomaly::AnomalyQueryResult>(variant));
        const auto& result = std::get<anomaly::AnomalyQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;

        auto groups = distinctGroupTags(result);
        ASSERT_EQ(groups.size(), 1u) << "a device with one observation must not be given an envelope";
        EXPECT_EQ(groups[0], (std::vector<std::string>{"dev=FULL"}));

        EXPECT_EQ(result.statistics.declinedSeriesCount, 1u)
            << "REGRESSION: the declined group vanished without a trace";
        EXPECT_EQ(result.statistics.totalPoints, kAxis)
            << "only the answered group's real points count towards total_points";

        const std::string json = executor.formatAnomalyResponse(result);
        EXPECT_NE(json.find("\"declined_series_count\":1"), std::string::npos) << json.substr(0, 400);
    })
        .join()
        .get();
}

// ===========================================================================
// Phase 3.6 / R2 end to end: a group whose usable points number exactly TWO is
// declined rather than given a zero-width band.
//
// Bug (with model='simple', which fits only the last half of the input): a
// group with 18 finite points in slots 0-17 and exactly 2 in slots 30-31 was
// INCLUDED, forecast a flat 1270 with upper - lower == 0.000000 at every
// forecast point, labelled ["dev=SPARSE"].  Two points determine a line
// exactly and carry NO uncertainty information, so the band around them is
// fabricated certainty -- the same failure the earlier finite-point gate
// exists to stop, simply relocated from k<2 to k==2.
// ===========================================================================
TEST_F(DerivedQueryExecutorSeastarTest, ATwoUsablePointGroupIsDeclinedNotGivenAZeroWidthBand) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kAxis = 40;

        // FULL carries a real residual about its trend.  A PERFECTLY straight
        // series would fit with residualStdDev == 0 and so would also show a
        // zero-width band -- honestly, because the model really does explain
        // every point over 20 usable observations.  That is a different thing
        // from the k == 2 case, where the width is zero because the fit has no
        // residual degrees of freedom at all, and the band-width assertion at
        // the end of this test must not conflate the two.
        std::vector<std::pair<uint64_t, double>> full;
        for (size_t i = 0; i < kAxis; ++i) {
            const double wobble = (i % 3 == 0) ? 2.0 : ((i % 3 == 1) ? -1.0 : 0.5);
            full.push_back({startNs + i * minuteNs, 100.0 + static_cast<double>(i) + wobble});
        }
        insertFloatSeries(eng.eng, "half.fleet", "v", {{"dev", "FULL"}}, full);

        // 18 points in the FIRST half (which model='simple' discards) and
        // exactly 2 in the second.
        std::vector<std::pair<uint64_t, double>> sparse;
        for (size_t i = 0; i < 18; ++i) {
            sparse.push_back({startNs + i * minuteNs, 900.0 + 3.0 * static_cast<double>(i)});
        }
        for (size_t i = 0; i < 2; ++i) {
            sparse.push_back({startNs + (30 + i) * minuteNs, 1200.0 + 7.0 * static_cast<double>(i)});
        }
        insertFloatSeries(eng.eng, "half.fleet", "v", {{"dev", "SPARSE"}}, sparse);

        const uint64_t endNs = startNs + kAxis * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant = executor
                           .executeFromJsonWithAnomaly(derivedJsonBodyForLeg("avg:half.fleet(v){} by {dev}",
                                                                             "forecast(a, 'linear', 2, model='simple')",
                                                                             startNs, endNs, R"("1m")"))
                           .get();

        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
        const auto& result = std::get<forecast::ForecastQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;

        auto groups = distinctGroupTags(result);
        for (const auto& tags : groups) {
            ASSERT_FALSE(tags.empty());
            EXPECT_NE(tags[0], "dev=SPARSE") << "REGRESSION: a 2-point fit was published with a zero-width band";
        }
        EXPECT_EQ(result.statistics.declinedSeriesCount, 1u);

        // No group in the response may carry a degenerate band: for every
        // forecast slot, upper must be strictly above lower.
        for (const auto& piece : result.series) {
            if (piece.piece != "upper") {
                continue;
            }
            const auto* lower = [&]() -> const forecast::ForecastSeriesPiece* {
                for (const auto& candidate : result.series) {
                    if (candidate.piece == "lower" && candidate.groupTags == piece.groupTags) {
                        return &candidate;
                    }
                }
                return nullptr;
            }();
            ASSERT_NE(lower, nullptr);
            ASSERT_EQ(piece.values.size(), lower->values.size());
            for (size_t i = 0; i < piece.values.size(); ++i) {
                if (!piece.values[i].has_value() || !lower->values[i].has_value()) {
                    continue;
                }
                EXPECT_GT(*piece.values[i] - *lower->values[i], 0.0)
                    << "zero-width band at slot " << i << " for group "
                    << (piece.groupTags.empty() ? "" : piece.groupTags[0]);
            }
        }
    })
        .join()
        .get();
}

// ===========================================================================
// Phase 3.8 / F2: a forecast whose OUTPUT would be enormous is REFUSED, not
// silently shortened.
//
// The horizon on this path is `duration / interval`, where interval is the
// leg's OWN sampling interval, so data that spans a minute inside a 30-day
// window asks for 30 days / 1 second == 2,592,000 forecast points.  MEASURED
// end to end against a pre-campaign build: 60 stored points (~480 bytes)
// produced a ~200 MB JSON body in 114 ms of server time, with reactor stalls
// of 442, 233, 124, 66 and 66 ms.  (Body size is FIXTURE-SPECIFIC: repeating
// the same 60@1s-in-30-days shape with different values measured 122.19 MB.
// Every slot is rendered as text, so the total tracks the decimal width of the
// values.  This test asserts on the POINT COUNT, which does not vary.)
// maxFanOutPoints cannot catch it -- the
// amplification is not a function of the input matrix -- so a caller refused by
// the cell bound could reach the same cost through a single-device leg.
//
// Phase 3.7 CLAMPED the horizon to max(2000, N) here.  That was wrong and this
// test replaces the two that pinned it (ForecastHorizonIsCappedWhenData...,
// ForecastHorizonCapDoesNotTouchALegSpanningItsWindow): a fixed ceiling cannot
// distinguish this leg from an ordinary long projection, so it truncated real
// forecasts with nothing in the response to say so -- 1440 one-minute points
// over a 30-day window went from 43,200 forecast points to 2,000.  The bound is
// on the OUTPUT SIZE instead, and it REFUSES rather than truncating: answer, or
// say why not.
// ===========================================================================
TEST_F(DerivedQueryExecutorSeastarTest, AForecastWhoseOutputWouldBeEnormousIsRefusedNotTruncated) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t secondNs = 1000000000ULL;
        const uint64_t dayNs = 86400000000000ULL;
        constexpr size_t kStored = 60;  // one minute of one-second samples

        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < kStored; ++i) {
            points.push_back({startNs + i * secondNs, 100.0 + 0.5 * static_cast<double>(i)});
        }
        insertFloatSeries(eng.eng, "sliver.metrics", "v", {{"dev", "D1"}}, points);

        // A 30-day window over a minute of data: duration / interval is
        // 2,592,000, so the output is 1 x (60 + 2,592,000).
        const uint64_t endNs = startNs + 30 * dayNs;

        DerivedQueryExecutor executor(&eng.eng);
        try {
            executor
                .executeFromJsonWithAnomaly(derivedJsonBodyForLeg("avg:sliver.metrics(v){dev:D1}",
                                                                  "forecast(a, 'linear', 2)", startNs, endNs, nullptr))
                .get();
            FAIL() << "REGRESSION: a 2.6-million-point forecast from " << kStored << " observations was answered";
        } catch (const DerivedQueryException& e) {
            const std::string what = e.what();
            // The numbers that tripped it, and which bound tripped.
            EXPECT_NE(what.find("2592060"), std::string::npos) << what;
            EXPECT_NE(what.find("2592000 forecast"), std::string::npos) << what;
            EXPECT_NE(what.find("60 historical"), std::string::npos) << what;
            EXPECT_NE(what.find("500000 point limit for a forecast result"), std::string::npos) << what;
            // ...and it is distinguishable from the cell bound, which speaks of
            // a "derived query result" and of series over a shared time axis.
            EXPECT_EQ(what.find("fans out to"), std::string::npos) << what;
        }
    })
        .join()
        .get();
}

// The other side of the bound: a leg whose data SPANS its window is answered in
// full, because duration / interval is then just the axis length.  1440
// one-minute buckets over a one-day window -- the reporter's own shape -- still
// projects 1439 points.
TEST_F(DerivedQueryExecutorSeastarTest, ForecastOutputBoundDoesNotTouchALegSpanningItsWindow) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kBuckets = 1440;  // one day of one-minute buckets

        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < kBuckets; ++i) {
            points.push_back({startNs + i * minuteNs, 100.0 + 0.01 * static_cast<double>(i)});
        }
        insertFloatSeries(eng.eng, "span.metrics", "v", {{"dev", "D1"}}, points);

        const uint64_t endNs = startNs + (kBuckets - 1) * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant = executor
                           .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                               "avg:span.metrics(v){dev:D1}", "forecast(a, 'linear', 2)", startNs, endNs, R"("1m")"))
                           .get();

        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
        const auto& result = std::get<forecast::ForecastQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;

        EXPECT_EQ(result.statistics.forecastPoints, kBuckets - 1)
            << "REGRESSION: the output bound shortened a forecast whose data spans its window";
    })
        .join()
        .get();
}

// The shape the 3.7 clamp actually damaged, pinned so it cannot come back: the
// SAME 1440 one-minute buckets projected over a 30-day window.  The honest
// answer is 43,200 forecast points (1 x (1440 + 43,200) == 44,640 output
// points, well inside the bound); the clamp answered 2,000 -- 33 hours of a
// 30-day ask -- and said nothing about it.
TEST_F(DerivedQueryExecutorSeastarTest, ALongProjectionFromADaysDataIsAnsweredInFull) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        const uint64_t dayNs = 86400000000000ULL;
        constexpr size_t kBuckets = 1440;

        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < kBuckets; ++i) {
            points.push_back({startNs + i * minuteNs, 100.0 + 0.01 * static_cast<double>(i)});
        }
        insertFloatSeries(eng.eng, "month.metrics", "v", {{"dev", "D1"}}, points);

        const uint64_t endNs = startNs + 30 * dayNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant = executor
                           .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                               "avg:month.metrics(v){dev:D1}", "forecast(a, 'linear', 2)", startNs, endNs, R"("1m")"))
                           .get();

        ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
        const auto& result = std::get<forecast::ForecastQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;

        EXPECT_EQ(result.statistics.forecastPoints, 43200u)
            << "REGRESSION: a legitimate 30-day projection was silently truncated";
    })
        .join()
        .get();
}

// The arithmetic that decides the bound must SATURATE, not wrap.  The horizon
// is `duration / interval` and nothing upstream bounds it: 15 points a
// NANOSECOND apart inside the widest window uint64 nanoseconds can express
// gives a horizon of 2^64 - 1, and a wrapping `historical + horizon` comes out
// as 14 -- comfortably inside the budget, so the single most extreme input in
// the whole space would be the one that slipped through and asked
// ForecastExecutor for 2^64 timestamps.
//
// REACHABLE OVER PLAIN JSON.  An earlier version of this note claimed the input
// was reachable "only through the protobuf write path", on the theory that a
// JSON timestamp goes through a double and so cannot express a 1 ns step at
// epoch magnitudes.  That is wrong, and wrong in the UNSAFE direction: it was
// reproduced end to end over POST /write with raw integer literals.  The write
// path parses JSON with glz::generic_u64 precisely so that nanosecond
// timestamps keep full uint64 precision (see lib/http/http_write_handler.hpp:
// 80-83 -- glz::json_t's num_mode::f64 would lose the low bits, which is why it
// is not used).  So this saturation is a guard on the ordinary JSON path, not a
// protobuf-only edge case.
TEST_F(DerivedQueryExecutorSeastarTest, ForecastOutputBoundSaturatesRatherThanWrapping) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        constexpr size_t kPoints = 15;

        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < kPoints; ++i) {
            points.push_back({startNs + i, 10.0 + static_cast<double>(i)});  // 1 ns apart
        }
        insertFloatSeries(eng.eng, "nano.metrics", "v", {{"dev", "D1"}}, points);

        DerivedQueryExecutor executor(&eng.eng);
        EXPECT_THROW(executor
                         .executeFromJsonWithAnomaly(
                             derivedJsonBodyForLeg("avg:nano.metrics(v){dev:D1}", "forecast(a, 'linear', 2)", 0,
                                                   std::numeric_limits<uint64_t>::max(), nullptr))
                         .get(),
                     DerivedQueryException)
            << "REGRESSION: a 2^64-point horizon wrapped past the output bound";
    })
        .join()
        .get();
}

// The bound counts OUTPUT, so it applies to a SINGLE-group leg -- unlike the
// cell bound in alignSubQueryGroups(), which exempts one group to keep
// returning data the user actually stored.  Forecast slots are not stored data.
// Pinned by turning the bound down rather than by building a huge leg.
TEST_F(DerivedQueryExecutorSeastarTest, ForecastOutputBoundAppliesToASingleGroupLeg) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kBuckets = 120;

        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < kBuckets; ++i) {
            points.push_back({startNs + i * minuteNs, 100.0 + 0.01 * static_cast<double>(i)});
        }
        insertFloatSeries(eng.eng, "single.metrics", "v", {{"dev", "D1"}}, points);

        const uint64_t endNs = startNs + (kBuckets - 1) * minuteNs;
        const std::string body = derivedJsonBodyForLeg("avg:single.metrics(v){dev:D1}", "forecast(a, 'linear', 2)",
                                                       startNs, endNs, R"("1m")");

        // 1 x (120 + 119) == 239 output points: fine by default...
        {
            DerivedQueryExecutor executor(&eng.eng);
            auto variant = executor.executeFromJsonWithAnomaly(body).get();
            ASSERT_TRUE(std::holds_alternative<forecast::ForecastQueryResult>(variant));
            EXPECT_TRUE(std::get<forecast::ForecastQueryResult>(variant).success);
        }

        // ...and refused when the budget is below it, one group notwithstanding.
        {
            DerivedQueryConfig tight;
            tight.maxForecastOutputPoints = 100;
            DerivedQueryExecutor executor(&eng.eng, tight);
            EXPECT_THROW(executor.executeFromJsonWithAnomaly(body).get(), DerivedQueryException)
                << "REGRESSION: the output bound exempted a single-group leg";
        }
    })
        .join()
        .get();
}

// ===========================================================================
// Phase 3.7 / S1 end to end: a DENSE single-series anomaly leg shorter than
// minDataPoints keeps returning its stored values.
//
// Phase 3.6's finite-point gate was on LENGTH, so `anomalies()` over a 5-point
// series answered `"series": []` where it had always returned five pieces --
// `raw` and `predictions` carrying the stored values, the envelope honestly
// all-null under the default BASIC algorithm used here (ROBUST has no warm-up
// and returns a finite band even at n = 3, which is equally its pre-campaign
// answer).  That is the standing single-series byte-identity guarantee, and a
// client plotting a short window lost its data to it.
// ===========================================================================
TEST_F(DerivedQueryExecutorSeastarTest, AShortDenseAnomalyLegStillReturnsItsStoredValues) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 5;  // half the default minDataPoints

        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < kPoints; ++i) {
            points.push_back({startNs + i * minuteNs, 10.0 + static_cast<double>(i)});
        }
        insertFloatSeries(eng.eng, "short.dense", "v", {{"dev", "D1"}}, points);

        const uint64_t endNs = startNs + (kPoints - 1) * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto variant = executor
                           .executeFromJsonWithAnomaly(derivedJsonBodyForLeg(
                               "avg:short.dense(v){dev:D1}", "anomalies(a, 'basic', 3)", startNs, endNs, nullptr))
                           .get();

        ASSERT_TRUE(std::holds_alternative<anomaly::AnomalyQueryResult>(variant));
        const auto& result = std::get<anomaly::AnomalyQueryResult>(variant);
        ASSERT_TRUE(result.success) << result.errorMessage;

        EXPECT_EQ(result.statistics.declinedSeriesCount, 0u)
            << "REGRESSION: a dense 5-point series was declined and its values dropped";
        EXPECT_EQ(result.statistics.totalPoints, kPoints);
        ASSERT_EQ(result.series.size(), 5u) << "raw, upper, lower, scores, predictions";

        const auto* raw = [&]() -> const anomaly::AnomalySeriesPiece* {
            for (const auto& piece : result.series) {
                if (piece.piece == "raw") {
                    return &piece;
                }
            }
            return nullptr;
        }();
        ASSERT_NE(raw, nullptr);
        ASSERT_EQ(raw->values.size(), kPoints);
        for (size_t i = 0; i < kPoints; ++i) {
            EXPECT_DOUBLE_EQ(raw->values[i], 10.0 + static_cast<double>(i));
        }

        const std::string json = executor.formatAnomalyResponse(result);
        EXPECT_EQ(json.find("declined_series_count"), std::string::npos) << json.substr(0, 300);
    })
        .join()
        .get();
}

// ===========================================================================
// Phase 4 — a plain ARITHMETIC formula over MANY series, per group.
//
// The customer's requirement in its narrowest form: "a + b should work on a
// per-field basis -- it works if a and b have the same fields, computing per
// field."  Generalised, a leg resolves to a map of (tag set, field) -> series
// and the formula is evaluated once per KEY, so a two-metric per-device
// dashboard is one request instead of one per device.
//
// The rules, each pinned below:
//   * PAIRING by (tag set, field) -- the key convertQueryResponseMulti()
//     already emits, so the arithmetic path and the forecast fan-out agree on
//     what a group IS.
//   * BROADCAST: a leg resolving to exactly one group is paired with every
//     group of the others (`per_device_bytes / fleet_total`).
//   * MISMATCH is a 400 naming the difference in BOTH directions, never a
//     silent intersection.
//   * The whole thing is behind the "multiSeries" opt-in, because
//     DerivedQueryResponse is FLAT: an older client reads only
//     timestamps/values and would take a multi-group answer for "no data".
// ===========================================================================

// A /derived body for two named legs, with the multiSeries flag optional.
static std::string derivedJsonBodyTwoLegs(const std::string& legA, const std::string& legB, const std::string& formula,
                                          uint64_t startNs, uint64_t endNs, bool multiSeries) {
    std::string json = R"json({"queries":{"a":")json" + legA + R"json(","b":")json" + legB +
                       R"json("},"formula":")json" + formula + R"json(","startTime":)json" + std::to_string(startNs) +
                       R"json(,"endTime":)json" + std::to_string(endNs);
    if (multiSeries) {
        json += R"json(,"multiSeries":true)json";
    }
    json += "}";
    return json;
}

// The per-device ratio fixture, chosen so that NO two groups share a value at
// ANY index: emitting row i against group j's tags changes every number, not
// just a label.
//
//   in.bytes  DEV-A = 100*(i+1)   DEV-B = 20*(i+1)   DEV-C = 3*(i+1)
//   out.bytes DEV-A = 10          DEV-B = 4          DEV-C = 1     (constant)
//   a / b     DEV-A = 10*(i+1)    DEV-B = 5*(i+1)    DEV-C = 3*(i+1)
//
// 10/5/3, 20/10/6, 30/15/9, ... -- pairwise distinct at every index.
struct RatioDevice {
    std::string device;
    double inStep;
    double outConst;
    double expectedStep;  // inStep / outConst
};

static const std::vector<RatioDevice>& ratioDevices() {
    static const std::vector<RatioDevice> devices = {
        {"DEV-A", 100.0, 10.0, 10.0},
        {"DEV-B", 20.0, 4.0, 5.0},
        {"DEV-C", 3.0, 1.0, 3.0},
    };
    return devices;
}

static void seedRatioFleet(seastar::sharded<Engine>& eng, uint64_t startNs, uint64_t minuteNs, size_t points) {
    for (const auto& dev : ratioDevices()) {
        std::vector<std::pair<uint64_t, double>> inPts;
        std::vector<std::pair<uint64_t, double>> outPts;
        for (size_t i = 0; i < points; ++i) {
            const uint64_t ts = startNs + i * minuteNs;
            inPts.push_back({ts, dev.inStep * static_cast<double>(i + 1)});
            outPts.push_back({ts, dev.outConst});
        }
        insertFloatSeries(eng, "netin", "bytes", {{"deviceId", dev.device}}, inPts);
        insertFloatSeries(eng, "netout", "bytes", {{"deviceId", dev.device}}, outPts);
    }
}

// ---- the flag is what decides: without it, the pre-Phase-4 400 stands ------
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesFlagAbsentKeepsTheSingleSeriesRefusal) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 5;
        seedRatioFleet(eng.eng, startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        try {
            executor
                .executeFromJson(derivedJsonBodyTwoLegs("avg:netin(bytes){} by {deviceId}",
                                                        "avg:netout(bytes){} by {deviceId}", "a / b", startNs, endNs,
                                                        /*multiSeries=*/false))
                .get();
            FAIL() << "Expected the pre-Phase-4 refusal without the multiSeries opt-in";
        } catch (const DerivedQueryException& e) {
            const std::string msg = e.what();
            EXPECT_NE(msg.find("exactly one series"), std::string::npos) << msg;
            EXPECT_NE(msg.find("Sub-query 'a'"), std::string::npos) << msg;
        }
    })
        .join()
        .get();
}

// ---- per-group evaluation: the values belong to the group they are labelled
// with, at every index ------------------------------------------------------
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesEvaluatesTheFormulaOncePerGroup) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 5;
        seedRatioFleet(eng.eng, startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto result = executor
                          .executeFromJson(derivedJsonBodyTwoLegs("avg:netin(bytes){} by {deviceId}",
                                                                  "avg:netout(bytes){} by {deviceId}", "a / b", startNs,
                                                                  endNs, /*multiSeries=*/true))
                          .get();

        ASSERT_EQ(result.series.size(), ratioDevices().size());

        // Tag sets ascending: DEV-A, DEV-B, DEV-C.  Single field across the
        // whole result, so NO "_field=" label.
        for (size_t g = 0; g < ratioDevices().size(); ++g) {
            const auto& dev = ratioDevices()[g];
            const auto& group = result.series[g];

            ASSERT_EQ(group.groupTags.size(), 1u) << "device " << dev.device;
            EXPECT_EQ(group.groupTags[0], "deviceId=" + dev.device);

            ASSERT_EQ(group.timestamps.size(), kPoints) << "device " << dev.device;
            ASSERT_EQ(group.values.size(), kPoints) << "device " << dev.device;
            for (size_t i = 0; i < kPoints; ++i) {
                EXPECT_EQ(group.timestamps[i], startNs + i * minuteNs) << dev.device << " i=" << i;
                // The whole point: a mispairing between tags and values shows
                // up here because no two devices agree at any index.
                EXPECT_DOUBLE_EQ(group.values[i], dev.expectedStep * static_cast<double>(i + 1))
                    << dev.device << " i=" << i;
            }
        }

        // More than one group: the FLAT columns stay empty and `series` is the
        // whole answer.
        EXPECT_TRUE(result.timestamps.empty());
        EXPECT_TRUE(result.values.empty());
        EXPECT_EQ(result.stats.pointCount, ratioDevices().size() * kPoints);
        EXPECT_EQ(result.stats.subQueriesExecuted, 2u);
    })
        .join()
        .get();
}

// ---- the customer's requirement verbatim: same fields on both legs, computed
// PER FIELD -------------------------------------------------------------------
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesPairsPerFieldWhenBothLegsNameTheSameFields) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 4;

        // ONE device, TWO fields per leg.  rx and tx are pulled far apart so a
        // field mispairing is a different number, not a rounding difference.
        for (size_t i = 0; i < kPoints; ++i) {
            const uint64_t ts = startNs + i * minuteNs;
            insertFloatSeries(eng.eng, "iface_in", "rx", {{"deviceId", "DEV-A"}},
                              {{ts, 100.0 * static_cast<double>(i + 1)}});
            insertFloatSeries(eng.eng, "iface_in", "tx", {{"deviceId", "DEV-A"}},
                              {{ts, 7.0 * static_cast<double>(i + 1)}});
            insertFloatSeries(eng.eng, "iface_out", "rx", {{"deviceId", "DEV-A"}}, {{ts, 4.0}});
            insertFloatSeries(eng.eng, "iface_out", "tx", {{"deviceId", "DEV-A"}}, {{ts, 7.0}});
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        // Grouped by deviceId so the response carries the tag: a SCOPE filter
        // alone narrows the series without labelling them (only a group-by key
        // survives into SeriesResult::tags), which would leave the groups
        // distinguished by their field label alone.
        auto result = executor
                          .executeFromJson(derivedJsonBodyTwoLegs("avg:iface_in(rx,tx){deviceId:DEV-A} by {deviceId}",
                                                                  "avg:iface_out(rx,tx){deviceId:DEV-A} by {deviceId}",
                                                                  "a / b", startNs, endNs,
                                                                  /*multiSeries=*/true))
                          .get();

        ASSERT_EQ(result.series.size(), 2u) << "one result per FIELD";

        // Requested order (rx, tx), and -- because the result spans two
        // distinct fields -- the synthetic "_field=" label, appended LAST.
        const std::vector<std::string> expectedFields = {"rx", "tx"};
        const std::vector<double> expectedStep = {25.0, 1.0};  // 100/4 and 7/7

        for (size_t g = 0; g < expectedFields.size(); ++g) {
            const auto& group = result.series[g];
            ASSERT_EQ(group.groupTags.size(), 2u);
            EXPECT_EQ(group.groupTags[0], "deviceId=DEV-A");
            EXPECT_EQ(group.groupTags[1], "_field=" + expectedFields[g]);

            ASSERT_EQ(group.values.size(), kPoints);
            for (size_t i = 0; i < kPoints; ++i) {
                EXPECT_DOUBLE_EQ(group.values[i], expectedStep[g] * static_cast<double>(i + 1))
                    << expectedFields[g] << " i=" << i;
            }
        }
    })
        .join()
        .get();
}

// ---- BROADCAST: a one-group leg pairs with every group of the other, in
// BOTH argument orders --------------------------------------------------------
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesBroadcastsAOneGroupLegInEitherOrder) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 5;
        seedRatioFleet(eng.eng, startNs, minuteNs, kPoints);

        // The fleet-total leg: ONE series, untagged, constant 2.0.
        for (size_t i = 0; i < kPoints; ++i) {
            insertFloatSeries(eng.eng, "fleet_total", "bytes", {}, {{startNs + i * minuteNs, 2.0}});
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);

        // per_device / fleet_total
        auto perOverTotal =
            executor
                .executeFromJson(derivedJsonBodyTwoLegs("avg:netin(bytes){} by {deviceId}", "avg:fleet_total(bytes){}",
                                                        "a / b", startNs, endNs, /*multiSeries=*/true))
                .get();
        ASSERT_EQ(perOverTotal.series.size(), ratioDevices().size())
            << "the single-series leg must broadcast, not collapse the result";

        // fleet_total / per_device -- the SAME groups, in the same order: the
        // reference leg is the multi-group one whichever side of the formula
        // it is on.
        auto totalOverPer =
            executor
                .executeFromJson(derivedJsonBodyTwoLegs("avg:fleet_total(bytes){}", "avg:netin(bytes){} by {deviceId}",
                                                        "a / b", startNs, endNs, /*multiSeries=*/true))
                .get();
        ASSERT_EQ(totalOverPer.series.size(), ratioDevices().size());

        for (size_t g = 0; g < ratioDevices().size(); ++g) {
            const auto& dev = ratioDevices()[g];
            ASSERT_EQ(perOverTotal.series[g].groupTags, std::vector<std::string>{"deviceId=" + dev.device});
            ASSERT_EQ(totalOverPer.series[g].groupTags, std::vector<std::string>{"deviceId=" + dev.device});

            ASSERT_EQ(perOverTotal.series[g].values.size(), kPoints);
            ASSERT_EQ(totalOverPer.series[g].values.size(), kPoints);
            for (size_t i = 0; i < kPoints; ++i) {
                const double perDevice = dev.inStep * static_cast<double>(i + 1);
                EXPECT_DOUBLE_EQ(perOverTotal.series[g].values[i], perDevice / 2.0) << dev.device << " i=" << i;
                EXPECT_DOUBLE_EQ(totalOverPer.series[g].values[i], 2.0 / perDevice) << dev.device << " i=" << i;
            }
        }
    })
        .join()
        .get();
}

// ---- MISMATCHED key sets are a 400 naming the difference BOTH ways ---------
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesMismatchedKeySetsAreALoudClientError) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 4;

        // a: DEV-A, DEV-B, DEV-C.   b: DEV-A, DEV-B, DEV-D.
        // So DEV-C is only in a and DEV-D is only in b -- both directions at
        // once, which is what makes silently intersecting to {A, B} so wrong.
        for (const char* device : {"DEV-A", "DEV-B", "DEV-C"}) {
            for (size_t i = 0; i < kPoints; ++i) {
                insertFloatSeries(eng.eng, "lhs", "v", {{"deviceId", device}}, {{startNs + i * minuteNs, 10.0}});
            }
        }
        for (const char* device : {"DEV-A", "DEV-B", "DEV-D"}) {
            for (size_t i = 0; i < kPoints; ++i) {
                insertFloatSeries(eng.eng, "rhs", "v", {{"deviceId", device}}, {{startNs + i * minuteNs, 2.0}});
            }
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        try {
            executor
                .executeFromJson(derivedJsonBodyTwoLegs("avg:lhs(v){} by {deviceId}", "avg:rhs(v){} by {deviceId}",
                                                        "a / b", startNs, endNs, /*multiSeries=*/true))
                .get();
            FAIL() << "Expected a 400: silently intersecting to {DEV-A, DEV-B} is the wrong answer";
        } catch (const DerivedQueryException& e) {
            const std::string msg = e.what();
            EXPECT_NE(msg.find("different series"), std::string::npos) << msg;
            EXPECT_NE(msg.find("DEV-C"), std::string::npos) << "must name what only 'a' has: " << msg;
            EXPECT_NE(msg.find("DEV-D"), std::string::npos) << "must name what only 'b' has: " << msg;
            EXPECT_NE(msg.find("in 'a' but not 'b'"), std::string::npos) << msg;
            EXPECT_NE(msg.find("in 'b' but not 'a'"), std::string::npos) << msg;
            EXPECT_NE(msg.find("broadcast"), std::string::npos) << "must be actionable: " << msg;
            EXPECT_EQ(msg.find("DEV-A"), std::string::npos) << "must not list the MATCHING keys: " << msg;
        }
    })
        .join()
        .get();
}

// ---- one group + the flag: the ARRAY ONLY, carrying exactly the points the
// no-flag path returns flat ---------------------------------------------------
//
// REPOINTED (was MultiSeriesSingleGroupPopulatesFlatAndArrayAlike, which pinned
// both forms being emitted together).  Emitting both doubled a one-group body --
// a measured 56.85 MB became 113.71 MB, and the one-group case is exempt from
// maxFanOutPoints so nothing capped it.  What still matters, and is still
// pinned here, is that the group carries EXACTLY the answer the flat path gives.
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesSingleGroupEmitsOnlyTheSeriesArray) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 5;
        seedRatioFleet(eng.eng, startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);

        // Grouped so the one group carries its tag (a scope filter alone does
        // not survive into the response's tag map).
        const std::string legA = "avg:netin(bytes){deviceId:DEV-B} by {deviceId}";
        const std::string legB = "avg:netout(bytes){deviceId:DEV-B} by {deviceId}";

        auto without =
            executor.executeFromJson(derivedJsonBodyTwoLegs(legA, legB, "a / b", startNs, endNs, false)).get();
        auto with = executor.executeFromJson(derivedJsonBodyTwoLegs(legA, legB, "a / b", startNs, endNs, true)).get();

        // Without the flag: flat only, exactly as it has always been.
        EXPECT_TRUE(without.series.empty());
        ASSERT_EQ(without.timestamps.size(), kPoints);

        // With the flag and ONE group: the ARRAY ONLY, carrying exactly the
        // points the flat form carries without it.  The flat columns stay empty
        // so the body is not paid for twice.
        ASSERT_EQ(with.series.size(), 1u);
        EXPECT_TRUE(with.timestamps.empty()) << "the flag replaces the flat columns, it does not duplicate them";
        EXPECT_TRUE(with.values.empty());
        EXPECT_EQ(with.series[0].timestamps, without.timestamps);
        EXPECT_EQ(with.series[0].values, without.values);
        EXPECT_EQ(with.stats.pointCount, without.stats.pointCount);
        EXPECT_EQ(with.stats.groupCount, 1u);

        // Single field, so no "_field=" label -- just the group's own tags.
        ASSERT_EQ(with.series[0].groupTags.size(), 1u);
        EXPECT_EQ(with.series[0].groupTags[0], "deviceId=DEV-B");

        // The JSON says the same: no "series" key at all without the flag, and
        // with it a "series" carrying the group -- once -- plus the group count.
        const std::string withoutJson = executor.formatResponse(without);
        EXPECT_EQ(withoutJson.find("\"series\""), std::string::npos) << withoutJson.substr(0, 400);
        EXPECT_EQ(withoutJson.find("\"group_count\""), std::string::npos) << withoutJson.substr(0, 400);
        const std::string withJson = executor.formatResponse(with);
        EXPECT_NE(withJson.find("\"series\""), std::string::npos) << withJson.substr(0, 400);
        EXPECT_NE(withJson.find("\"group_tags\":[\"deviceId=DEV-B\"]"), std::string::npos) << withJson.substr(0, 400);
        EXPECT_NE(withJson.find("\"group_count\":1"), std::string::npos) << withJson.substr(0, 400);
        // The points appear ONCE: the flat columns are empty arrays.
        EXPECT_NE(withJson.find("\"timestamps\":[],\"values\":[]"), std::string::npos) << withJson.substr(0, 400);
    })
        .join()
        .get();
}

// ---- the output bound: groups x per-group axis, as a CLIENT error ----------
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesOutputBoundIsAClientErrorAndExemptsOneGroup) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 5;
        seedRatioFleet(eng.eng, startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        // 3 groups x 5 points = 15 cells, over a bound of 7.
        DerivedQueryConfig tight;
        tight.maxFanOutPoints = 7;
        DerivedQueryExecutor capped(&eng.eng, tight);

        try {
            capped
                .executeFromJson(derivedJsonBodyTwoLegs("avg:netin(bytes){} by {deviceId}",
                                                        "avg:netout(bytes){} by {deviceId}", "a / b", startNs, endNs,
                                                        /*multiSeries=*/true))
                .get();
            FAIL() << "Expected the fan-out point bound to refuse this";
        } catch (const DerivedQueryException& e) {
            const std::string msg = e.what();
            EXPECT_NE(msg.find("point limit"), std::string::npos) << msg;
            EXPECT_NE(msg.find("7"), std::string::npos) << "must state the limit: " << msg;
            EXPECT_NE(msg.find("aggregationInterval"), std::string::npos) << "must be actionable: " << msg;
        }

        // A ONE-GROUP result is exempt, exactly as it is on the forecast
        // fan-out: that is the answer this endpoint gave before the flag
        // existed and it must not start 400ing.
        auto single = capped
                          .executeFromJson(derivedJsonBodyTwoLegs("avg:netin(bytes){deviceId:DEV-A}",
                                                                  "avg:netout(bytes){deviceId:DEV-A}", "a / b", startNs,
                                                                  endNs, /*multiSeries=*/true))
                          .get();
        ASSERT_EQ(single.series.size(), 1u);
        EXPECT_EQ(single.series[0].values.size(), kPoints) << "5 points under a bound of 7 cells, and exempt anyway";

        // And with room, the three-group query answers in full.
        DerivedQueryConfig roomy;
        roomy.maxFanOutPoints = 100;
        DerivedQueryExecutor ok(&eng.eng, roomy);
        auto full = ok.executeFromJson(derivedJsonBodyTwoLegs("avg:netin(bytes){} by {deviceId}",
                                                              "avg:netout(bytes){} by {deviceId}", "a / b", startNs,
                                                              endNs, /*multiSeries=*/true))
                        .get();
        EXPECT_EQ(full.series.size(), ratioDevices().size());
        EXPECT_EQ(full.stats.pointCount, ratioDevices().size() * kPoints);
    })
        .join()
        .get();
}

// ---- a leg that matched NOTHING is an empty result, not a mismatch ---------
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesEmptyLegYieldsAnEmptyResultNotAMismatch) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 5;
        seedRatioFleet(eng.eng, startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto result = executor
                          .executeFromJson(derivedJsonBodyTwoLegs("avg:netin(bytes){} by {deviceId}",
                                                                  "avg:nosuchthing(bytes){} by {deviceId}", "a / b",
                                                                  startNs, endNs, /*multiSeries=*/true))
                          .get();

        EXPECT_TRUE(result.series.empty());
        EXPECT_TRUE(result.timestamps.empty());
        EXPECT_EQ(result.stats.pointCount, 0u);
    })
        .join()
        .get();
}

// ---- ORDER is the reference leg's, and the reference leg is NAMED ----------
//
// `a = in(tx,rx)` and `b = out(rx,tx)` rank the same two fields oppositely, so
// "the order of a" and "the order of b" are different answers.  The rule picks
// the first leg BY NAME that resolves to more than one group -- here 'a' --
// which is what makes the sequence a pure function of the request rather than
// of which shard answered first.
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesOutputOrderFollowsTheReferenceLeg) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 3;
        for (size_t i = 0; i < kPoints; ++i) {
            const uint64_t ts = startNs + i * minuteNs;
            insertFloatSeries(eng.eng, "ord_in", "rx", {}, {{ts, 60.0}});
            insertFloatSeries(eng.eng, "ord_in", "tx", {}, {{ts, 30.0}});
            insertFloatSeries(eng.eng, "ord_out", "rx", {}, {{ts, 6.0}});
            insertFloatSeries(eng.eng, "ord_out", "tx", {}, {{ts, 3.0}});
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto result = executor
                          .executeFromJson(derivedJsonBodyTwoLegs("avg:ord_in(tx,rx){}", "avg:ord_out(rx,tx){}",
                                                                  "a / b", startNs, endNs, /*multiSeries=*/true))
                          .get();

        ASSERT_EQ(result.series.size(), 2u);
        // 'a' named tx first, so tx comes first -- and the untagged groups
        // carry ONLY the synthetic field label.
        ASSERT_EQ(result.series[0].groupTags, std::vector<std::string>{"_field=tx"});
        ASSERT_EQ(result.series[1].groupTags, std::vector<std::string>{"_field=rx"});
        ASSERT_EQ(result.series[0].values.size(), kPoints);
        EXPECT_DOUBLE_EQ(result.series[0].values[0], 10.0);  // tx: 30 / 3
        EXPECT_DOUBLE_EQ(result.series[1].values[0], 10.0);  // rx: 60 / 6

        // Repeating the identical request yields the identical sequence.
        auto again = executor
                         .executeFromJson(derivedJsonBodyTwoLegs("avg:ord_in(tx,rx){}", "avg:ord_out(rx,tx){}", "a / b",
                                                                 startNs, endNs, /*multiSeries=*/true))
                         .get();
        ASSERT_EQ(again.series.size(), 2u);
        EXPECT_EQ(again.series[0].groupTags, result.series[0].groupTags);
        EXPECT_EQ(again.series[1].groupTags, result.series[1].groupTags);
    })
        .join()
        .get();
}

// ---- the flag does not disturb a THREE-leg formula, nor the broadcast of two
// single-group legs against one multi-group leg -------------------------------
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesBroadcastsSeveralSingleGroupLegsAtOnce) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 4;
        seedRatioFleet(eng.eng, startNs, minuteNs, kPoints);
        for (size_t i = 0; i < kPoints; ++i) {
            insertFloatSeries(eng.eng, "fleet_total", "bytes", {}, {{startNs + i * minuteNs, 2.0}});
            insertFloatSeries(eng.eng, "fleet_cap", "bytes", {}, {{startNs + i * minuteNs, 5.0}});
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        std::string json = R"json({"queries":{)json"
                           R"json("a":"avg:netin(bytes){} by {deviceId}",)json"
                           R"json("b":"avg:fleet_total(bytes){}",)json"
                           R"json("c":"avg:fleet_cap(bytes){}"},)json"
                           R"json("formula":"a / (b * c)","startTime":)json" +
                           std::to_string(startNs) + R"json(,"endTime":)json" + std::to_string(endNs) +
                           R"json(,"multiSeries":true})json";

        DerivedQueryExecutor executor(&eng.eng);
        auto result = executor.executeFromJson(json).get();

        ASSERT_EQ(result.series.size(), ratioDevices().size());
        EXPECT_EQ(result.stats.subQueriesExecuted, 3u);
        for (size_t g = 0; g < ratioDevices().size(); ++g) {
            const auto& dev = ratioDevices()[g];
            ASSERT_EQ(result.series[g].groupTags, std::vector<std::string>{"deviceId=" + dev.device});
            ASSERT_EQ(result.series[g].values.size(), kPoints);
            for (size_t i = 0; i < kPoints; ++i) {
                EXPECT_DOUBLE_EQ(result.series[g].values[i], dev.inStep * static_cast<double>(i + 1) / 10.0)
                    << dev.device << " i=" << i;
            }
        }
    })
        .join()
        .get();
}

// ============================================================================
// Phase 4.5: the multi-series path's pairing, budget and response shape
// ============================================================================

// ---- D1: a TAGGED one-group leg does not broadcast over groups it is not
// part of ---------------------------------------------------------------------
//
// The defect this pins: broadcast used to fire on ARITY alone, so
// `a = avg:netin(bytes){} by {deviceId}` (DEV-A/B/C) over
// `b = avg:netout(bytes){deviceId:DEV-A} by {deviceId}` (one group, tagged
// DEV-A) answered HTTP 200 with three groups -- the one LABELLED deviceId=DEV-B
// holding netin[DEV-B] / netout[DEV-A].  b provably carries a tag set that is
// not DEV-B's, so the label asserted a pairing that never happened, and the
// over-narrow scope filter that caused it is exactly what the mismatch 400
// exists to catch.
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesTaggedOneGroupLegDoesNotBroadcast) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 4;
        seedRatioFleet(eng.eng, startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);

        // Both argument orders: the tagged single group is refused whichever
        // side of the formula it is on.
        const std::vector<std::pair<std::string, std::string>> orders = {
            {"avg:netin(bytes){} by {deviceId}", "avg:netout(bytes){deviceId:DEV-A} by {deviceId}"},
            {"avg:netout(bytes){deviceId:DEV-A} by {deviceId}", "avg:netin(bytes){} by {deviceId}"},
        };

        for (const auto& [legA, legB] : orders) {
            try {
                auto wrong =
                    executor.executeFromJson(derivedJsonBodyTwoLegs(legA, legB, "a / b", startNs, endNs, true)).get();
                ADD_FAILURE() << "Expected a 400: instead got " << wrong.series.size()
                              << " groups, the second of which claims a pairing that never happened";
            } catch (const DerivedQueryException& e) {
                const std::string msg = e.what();
                EXPECT_NE(msg.find("deviceId=DEV-A"), std::string::npos)
                    << "must name the tags that do not fit: " << msg;
                EXPECT_NE(msg.find("subset"), std::string::npos) << "must state the broadcast rule: " << msg;
                EXPECT_NE(msg.find("DEV-B"), std::string::npos) << "must name the groups it cannot cover: " << msg;
            }
        }
    })
        .join()
        .get();
}

// ---- D1, the other half: EMPTY and SUBSET tag sets still broadcast ---------
//
// The documented motivation (`per_device / fleet_total`) is an UNTAGGED single
// group, and a leg scoped to a tag every output group shares is the same shape
// one level down.  Both must keep working, or the fix for the mispairing above
// would have broken the feature it guards.
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesSubsetTaggedOneGroupLegStillBroadcasts) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 4;

        // Per-device series carry BOTH deviceId and rack; the rack total carries
        // only rack -- a strict subset of every output group's tags.
        const std::vector<std::pair<std::string, double>> devices = {{"DEV-A", 100.0}, {"DEV-B", 20.0}};
        for (size_t i = 0; i < kPoints; ++i) {
            const uint64_t ts = startNs + i * minuteNs;
            for (const auto& [device, step] : devices) {
                insertFloatSeries(eng.eng, "rk_dev", "bytes", {{"deviceId", device}, {"rack", "R1"}},
                                  {{ts, step * static_cast<double>(i + 1)}});
            }
            insertFloatSeries(eng.eng, "rk_total", "bytes", {{"rack", "R1"}}, {{ts, 4.0}});
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto result = executor
                          .executeFromJson(derivedJsonBodyTwoLegs("avg:rk_dev(bytes){} by {deviceId,rack}",
                                                                  "avg:rk_total(bytes){} by {rack}", "a / b", startNs,
                                                                  endNs, /*multiSeries=*/true))
                          .get();

        ASSERT_EQ(result.series.size(), devices.size());
        for (size_t g = 0; g < devices.size(); ++g) {
            const auto& [device, step] = devices[g];
            ASSERT_EQ(result.series[g].groupTags, (std::vector<std::string>{"deviceId=" + device, "rack=R1"}));
            ASSERT_EQ(result.series[g].values.size(), kPoints);
            for (size_t i = 0; i < kPoints; ++i) {
                EXPECT_DOUBLE_EQ(result.series[g].values[i], step * static_cast<double>(i + 1) / 4.0)
                    << device << " i=" << i;
            }
        }
    })
        .join()
        .get();
}

// ---- D5: cross-measurement pairing, by TAGS alone -------------------------
//
// `cpu.user / mem.used by {host}` is one series per host on each side and is
// unambiguously paired by host, but the (tags, field) key refused it with advice
// no caller could act on: no scope filter renames a field, and reducing either
// side to one series is the very thing this campaign exists to stop requiring.
// When EVERY leg resolves to exactly one distinct field name, the field carries
// no information and the key is the tag set alone.
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesPairsCrossMeasurementFieldsByTagAlone) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 3;

        // Values chosen so a HOST mispairing is a different number at every
        // index, not a rounding difference.
        const std::vector<std::tuple<std::string, double, double>> hosts = {
            {"h1", 90.0, 3.0},
            {"h2", 40.0, 8.0},
        };
        for (size_t i = 0; i < kPoints; ++i) {
            const uint64_t ts = startNs + i * minuteNs;
            for (const auto& [host, cpu, mem] : hosts) {
                insertFloatSeries(eng.eng, "cpu", "user", {{"host", host}}, {{ts, cpu * static_cast<double>(i + 1)}});
                insertFloatSeries(eng.eng, "mem", "used", {{"host", host}}, {{ts, mem}});
            }
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto result = executor
                          .executeFromJson(derivedJsonBodyTwoLegs("avg:cpu(user){} by {host}",
                                                                  "avg:mem(used){} by {host}", "a / b", startNs, endNs,
                                                                  /*multiSeries=*/true))
                          .get();

        ASSERT_EQ(result.series.size(), hosts.size()) << "one group per host, paired across measurements";
        for (size_t g = 0; g < hosts.size(); ++g) {
            const auto& [host, cpu, mem] = hosts[g];
            // ONE distinct field in the result, so NO "_field=" label -- the
            // label rule is unchanged.
            ASSERT_EQ(result.series[g].groupTags, std::vector<std::string>{"host=" + host});
            ASSERT_EQ(result.series[g].values.size(), kPoints);
            for (size_t i = 0; i < kPoints; ++i) {
                EXPECT_DOUBLE_EQ(result.series[g].values[i], cpu * static_cast<double>(i + 1) / mem)
                    << host << " i=" << i;
            }
        }
    })
        .join()
        .get();
}

// ---- D5 does NOT weaken per-field pairing ---------------------------------
//
// The tags-only key applies only when every leg names exactly one field.  As
// soon as a leg names two, the field is back in the key -- so two multi-group
// legs naming DIFFERENT fields are the mismatch they always were, not a silent
// fold of tx onto ty.
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesKeepsTheFieldInTheKeyWhenALegNamesTwo) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 3;
        for (size_t i = 0; i < kPoints; ++i) {
            const uint64_t ts = startNs + i * minuteNs;
            insertFloatSeries(eng.eng, "two_in", "rx", {{"deviceId", "DEV-A"}}, {{ts, 10.0}});
            insertFloatSeries(eng.eng, "two_in", "tx", {{"deviceId", "DEV-A"}}, {{ts, 20.0}});
            insertFloatSeries(eng.eng, "two_out", "rx", {{"deviceId", "DEV-A"}}, {{ts, 2.0}});
            insertFloatSeries(eng.eng, "two_out", "ty", {{"deviceId", "DEV-A"}}, {{ts, 4.0}});
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        try {
            executor
                .executeFromJson(derivedJsonBodyTwoLegs("avg:two_in(rx,tx){} by {deviceId}",
                                                        "avg:two_out(rx,ty){} by {deviceId}", "a / b", startNs, endNs,
                                                        /*multiSeries=*/true))
                .get();
            FAIL() << "Expected a 400: tx and ty are different fields and must not be paired";
        } catch (const DerivedQueryException& e) {
            const std::string msg = e.what();
            EXPECT_NE(msg.find("_field=tx"), std::string::npos) << "must name the unpaired field: " << msg;
            EXPECT_NE(msg.find("_field=ty"), std::string::npos) << "in both directions: " << msg;
        }
    })
        .join()
        .get();
}

// ---- D3: the budget is checked BEFORE a group is materialised -------------
//
// Two devices with four stored points between them, bucketed at one second over
// a thirty-day window: SeriesAligner::resampleTimestamps() would build a dense
// grid of 2,592,001 slots PER GROUP.  The old incremental check ran after
// alignment, evaluation and push_back, and never examined the first group at
// all, so the first 2.59M points (10.4x the budget) were built -- with a
// reactor stall -- before anything refused them.
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesBoundRefusesBeforeMaterialisingTheFirstGroup) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t secondNs = 1000000000ULL;
        const uint64_t thirtyDaysNs = 30ULL * 24 * 3600 * secondNs;

        // Two points per device, thirty days apart -- four stored points total.
        for (const char* device : {"DEV-A", "DEV-B"}) {
            insertFloatSeries(eng.eng, "sparse_in", "v", {{"deviceId", device}},
                              {{startNs, 10.0}, {startNs + thirtyDaysNs, 20.0}});
            insertFloatSeries(eng.eng, "sparse_out", "v", {{"deviceId", device}},
                              {{startNs, 2.0}, {startNs + thirtyDaysNs, 4.0}});
        }

        std::string json = R"json({"queries":{"a":"avg:sparse_in(v){} by {deviceId}",)json"
                           R"json("b":"avg:sparse_out(v){} by {deviceId}"},)json"
                           R"json("formula":"a / b","startTime":)json" +
                           std::to_string(startNs) + R"json(,"endTime":)json" + std::to_string(startNs + thirtyDaysNs) +
                           R"json(,"aggregationInterval":"1s","multiSeries":true})json";

        DerivedQueryExecutor executor(&eng.eng);
        const auto began = std::chrono::steady_clock::now();
        try {
            auto materialised = executor.executeFromJson(json).get();
            FAIL() << "Expected the fan-out bound to refuse " << materialised.stats.pointCount << " points";
        } catch (const DerivedQueryException& e) {
            const std::string msg = e.what();
            const double elapsedMs =
                std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - began).count();

            // The PRE-check's wording, not the backstop's: the backstop can only
            // speak of points it has already built.
            EXPECT_NE(msg.find("would produce at least"), std::string::npos) << msg;
            EXPECT_EQ(msg.find("has already produced"), std::string::npos)
                << "refused only after building the group: " << msg;
            // It knows the full grid size without having built it.
            EXPECT_NE(msg.find("2592001"), std::string::npos) << msg;
            EXPECT_NE(msg.find("aggregationInterval"), std::string::npos) << "must be actionable: " << msg;
            // Building 2.59M points took ~137 ms per 400k in the report; the
            // projection is arithmetic on four timestamps.
            EXPECT_LT(elapsedMs, 2000.0) << "refusal should not cost what materialising would have";
        }
    })
        .join()
        .get();
}

// ---- D6: an opted-in empty answer says `series: []`, not nothing ----------
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesEmptyAnswerStillCarriesTheArrayAndGroupCount) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 3;
        seedRatioFleet(eng.eng, startNs, minuteNs, kPoints);
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto empty = executor
                         .executeFromJson(derivedJsonBodyTwoLegs("avg:netin(bytes){} by {deviceId}",
                                                                 "avg:nosuchthing(bytes){} by {deviceId}", "a / b",
                                                                 startNs, endNs, /*multiSeries=*/true))
                         .get();
        EXPECT_TRUE(empty.series.empty());
        EXPECT_TRUE(empty.multiSeries) << "the response shape is decided by the request, not by what matched";

        const std::string json = executor.formatResponse(empty);
        EXPECT_NE(json.find("\"series\":[]"), std::string::npos)
            << "a multiSeries client must not have to handle `undefined`: " << json;
        EXPECT_NE(json.find("\"group_count\":0"), std::string::npos) << json;
    })
        .join()
        .get();
}

// ---- D6: points_dropped_due_to_alignment is a SUM across groups ------------
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesAlignmentDropsAreSummedAcrossGroups) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;

        // Per device: 'in' at t0,t1,t2 and 'out' at t1,t2,t3.  INNER alignment
        // keeps {t1,t2}, dropping one point from each leg -- 2 per group.
        for (const char* device : {"DEV-A", "DEV-B"}) {
            insertFloatSeries(eng.eng, "stag_in", "v", {{"deviceId", device}},
                              {{startNs, 10.0}, {startNs + minuteNs, 20.0}, {startNs + 2 * minuteNs, 30.0}});
            insertFloatSeries(
                eng.eng, "stag_out", "v", {{"deviceId", device}},
                {{startNs + minuteNs, 2.0}, {startNs + 2 * minuteNs, 2.0}, {startNs + 3 * minuteNs, 2.0}});
        }
        const uint64_t endNs = startNs + 4 * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto result = executor
                          .executeFromJson(derivedJsonBodyTwoLegs("avg:stag_in(v){} by {deviceId}",
                                                                  "avg:stag_out(v){} by {deviceId}", "a / b", startNs,
                                                                  endNs, /*multiSeries=*/true))
                          .get();

        ASSERT_EQ(result.series.size(), 2u);
        EXPECT_EQ(result.series[0].values.size(), 2u);
        EXPECT_EQ(result.stats.groupCount, 2u);
        // 2 per group x 2 groups: a TOTAL, not a per-group figure.
        EXPECT_EQ(result.stats.pointsDroppedDueToAlignment, 4u);
    })
        .join()
        .get();
}

// ---- no fan-out: two single-series legs pair regardless of tags ------------
//
// The counterpart to MultiSeriesTaggedOneGroupLegDoesNotBroadcast.  D1's defect
// is ONE group standing in for MANY it does not describe; when every leg
// resolves to one group there is no fan-out and nothing to stand in for, so
// comparing two named hosts keeps answering exactly as it does without the flag.
// What the flag adds is a LABEL, and a label naming one of the two hosts would
// be the same false claim -- so the group carries none.
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesTwoSingleLegsWithDifferentTagsPairAndCarryNoLabel) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 4;
        for (size_t i = 0; i < kPoints; ++i) {
            const uint64_t ts = startNs + i * minuteNs;
            insertFloatSeries(eng.eng, "hcpu", "v", {{"host", "web1"}}, {{ts, 90.0}});
            insertFloatSeries(eng.eng, "hcpu", "v", {{"host", "web2"}}, {{ts, 25.0}});
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        const std::string legA = "avg:hcpu(v){host:web1} by {host}";
        const std::string legB = "avg:hcpu(v){host:web2} by {host}";

        // Without the flag this has always worked; with it, it still does.
        auto without =
            executor.executeFromJson(derivedJsonBodyTwoLegs(legA, legB, "a - b", startNs, endNs, false)).get();
        ASSERT_EQ(without.values.size(), kPoints);
        EXPECT_DOUBLE_EQ(without.values[0], 65.0);

        auto with = executor.executeFromJson(derivedJsonBodyTwoLegs(legA, legB, "a - b", startNs, endNs, true)).get();
        ASSERT_EQ(with.series.size(), 1u) << "one group in from each leg, one group out";
        EXPECT_TRUE(with.series[0].groupTags.empty())
            << "labelling the result host=web1 would assert it came from a series that supplied half of it";
        ASSERT_EQ(with.series[0].values.size(), kPoints);
        for (size_t i = 0; i < kPoints; ++i) {
            EXPECT_DOUBLE_EQ(with.series[0].values[i], 65.0) << "i=" << i;
        }
        EXPECT_EQ(with.series[0].values, without.values) << "the flag changes the shape, never the answer";
        EXPECT_EQ(with.stats.groupCount, 1u);

        // The JSON carries the empty label rather than omitting group_tags.
        const std::string json = executor.formatResponse(with);
        EXPECT_NE(json.find("\"group_tags\":[]"), std::string::npos) << json.substr(0, 400);
    })
        .join()
        .get();
}

// ---- ... and legs that AGREE on their tags keep them -----------------------
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesTwoSingleLegsWithIdenticalTagsKeepThem) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 4;
        for (size_t i = 0; i < kPoints; ++i) {
            const uint64_t ts = startNs + i * minuteNs;
            insertFloatSeries(eng.eng, "hin", "v", {{"host", "web1"}}, {{ts, 90.0}});
            insertFloatSeries(eng.eng, "hout", "v", {{"host", "web1"}}, {{ts, 3.0}});
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto result = executor
                          .executeFromJson(derivedJsonBodyTwoLegs("avg:hin(v){} by {host}", "avg:hout(v){} by {host}",
                                                                  "a / b", startNs, endNs, /*multiSeries=*/true))
                          .get();

        ASSERT_EQ(result.series.size(), 1u);
        EXPECT_EQ(result.series[0].groupTags, std::vector<std::string>{"host=web1"})
            << "every leg agreed on the tag set, so the group may carry it";
        ASSERT_EQ(result.series[0].values.size(), kPoints);
        EXPECT_DOUBLE_EQ(result.series[0].values[0], 30.0);
    })
        .join()
        .get();
}

// ---- a leg with tags the others lack still keeps them when they AGREE with
// it -- the identity test is over the legs, not over "has any tags" ----------
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesUntaggedSingleLegsDropNoLabelTheyNeverHad) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 3;
        for (size_t i = 0; i < kPoints; ++i) {
            const uint64_t ts = startNs + i * minuteNs;
            insertFloatSeries(eng.eng, "untag_in", "v", {}, {{ts, 12.0}});
            insertFloatSeries(eng.eng, "untag_out", "v", {}, {{ts, 4.0}});
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto result = executor
                          .executeFromJson(derivedJsonBodyTwoLegs("avg:untag_in(v){}", "avg:untag_out(v){}", "a / b",
                                                                  startNs, endNs, /*multiSeries=*/true))
                          .get();

        ASSERT_EQ(result.series.size(), 1u);
        EXPECT_TRUE(result.series[0].groupTags.empty()) << "both legs are untagged: identical, and identically empty";
        EXPECT_DOUBLE_EQ(result.series[0].values[0], 3.0);
    })
        .join()
        .get();
}

// ============================================================================
// Phase 4.6: the broadcast rule on the FIELD axis, and an EXACT output bound
// ============================================================================

static std::string derivedJsonBodyTwoLegsWithInterval(const std::string& legA, const std::string& legB,
                                                      const std::string& formula, uint64_t startNs, uint64_t endNs,
                                                      const std::string& interval) {
    std::string json = R"json({"queries":{"a":")json" + legA + R"json(","b":")json" + legB +
                       R"json("},"formula":")json" + formula + R"json(","startTime":)json" + std::to_string(startNs) +
                       R"json(,"endTime":)json" + std::to_string(endNs) + R"json(,"aggregationInterval":")json" +
                       interval + R"json(","multiSeries":true})json";
    return json;
}

// ---- B1: the D1 hole, one axis over ---------------------------------------
//
// Broadcast was admitted on the TAG set alone, so a one-group leg whose tags
// matched every output group still broadcast when the FIELD did not:
//
//   a = avg:iface_in(rx,tx){} by {deviceId}  -> (DEV-A, rx), (DEV-A, tx)
//   b = avg:iface_out(rx){} by {deviceId}    -> one group, field rx
//
// answered 200 with a group LABELLED `_field=tx` holding in.tx / out.RX -- the
// same false claim the tagged case is refused for, and the message that refuses
// it is its own argument against allowing this.
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesOneGroupLegWhoseFieldMatchesSomeGroupsDoesNotBroadcast) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 3;
        for (size_t i = 0; i < kPoints; ++i) {
            const uint64_t ts = startNs + i * minuteNs;
            insertFloatSeries(eng.eng, "fx_in", "rx", {{"deviceId", "DEV-A"}}, {{ts, 10.0}});
            insertFloatSeries(eng.eng, "fx_in", "tx", {{"deviceId", "DEV-A"}}, {{ts, 20.0}});
            insertFloatSeries(eng.eng, "fx_out", "rx", {{"deviceId", "DEV-A"}}, {{ts, 2.0}});
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);

        // Both argument orders: the reference leg is the multi-group one either
        // way, so the single group is tested either way.
        const std::vector<std::pair<std::string, std::string>> orders = {
            {"avg:fx_in(rx,tx){} by {deviceId}", "avg:fx_out(rx){} by {deviceId}"},
            {"avg:fx_out(rx){} by {deviceId}", "avg:fx_in(rx,tx){} by {deviceId}"},
        };

        for (const auto& [legA, legB] : orders) {
            try {
                auto wrong =
                    executor.executeFromJson(derivedJsonBodyTwoLegs(legA, legB, "a / b", startNs, endNs, true)).get();
                ADD_FAILURE() << "Expected a 400: instead got " << wrong.series.size()
                              << " groups, one of them labelled with a field its denominator never carried";
            } catch (const DerivedQueryException& e) {
                const std::string msg = e.what();
                EXPECT_NE(msg.find("field 'rx'"), std::string::npos) << "must name the offending field: " << msg;
                EXPECT_NE(msg.find("_field=tx"), std::string::npos) << "must name the group it cannot cover: " << msg;
                EXPECT_NE(msg.find("some but not all"), std::string::npos) << "must state the rule: " << msg;
            }
        }
    })
        .join()
        .get();
}

// ---- B1, the other half: a field named by NO output group still broadcasts -
//
// The common scalar denominator (`net(rx,tx) / total(bytes)`) is the reason the
// rule is not field EQUALITY: `bytes` matches neither output key, describes both
// equally, and the answer is computed per field.  Refusing it would have broken
// the feature the field rule guards.
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesScalarDenominatorWithItsOwnFieldStillBroadcasts) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 3;
        for (size_t i = 0; i < kPoints; ++i) {
            const uint64_t ts = startNs + i * minuteNs;
            insertFloatSeries(eng.eng, "sd_net", "rx", {{"deviceId", "DEV-A"}}, {{ts, 10.0}});
            insertFloatSeries(eng.eng, "sd_net", "tx", {{"deviceId", "DEV-A"}}, {{ts, 20.0}});
            insertFloatSeries(eng.eng, "sd_total", "bytes", {}, {{ts, 4.0}});
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto result = executor
                          .executeFromJson(derivedJsonBodyTwoLegs("avg:sd_net(rx,tx){} by {deviceId}",
                                                                  "avg:sd_total(bytes){}", "a / b", startNs, endNs,
                                                                  /*multiSeries=*/true))
                          .get();

        ASSERT_EQ(result.series.size(), 2u) << "one group per field of the fanned-out leg";
        EXPECT_EQ(result.series[0].groupTags, (std::vector<std::string>{"deviceId=DEV-A", "_field=rx"}));
        EXPECT_EQ(result.series[1].groupTags, (std::vector<std::string>{"deviceId=DEV-A", "_field=tx"}));
        ASSERT_EQ(result.series[0].values.size(), kPoints);
        ASSERT_EQ(result.series[1].values.size(), kPoints);
        for (size_t i = 0; i < kPoints; ++i) {
            EXPECT_DOUBLE_EQ(result.series[0].values[i], 10.0 / 4.0) << "rx i=" << i;
            EXPECT_DOUBLE_EQ(result.series[1].values[i], 20.0 / 4.0) << "tx i=" << i;
        }
    })
        .join()
        .get();
}

// ---- B2: the output bound counts the points the group will REALLY have ------
//
// The pre-check used to size the resample grid over [max(first), min(last)] --
// an interval that merely CONTAINS the intersection.  Two legs sharing a dense
// band of timestamps but differing at both ends of a wide window were therefore
// refused for 299,999 points when the real answer was 100 per group: a false
// 400 on a query that costs nothing, and a regression against the post-hoc check
// this pre-check replaced (which counted actual points and answered it).
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesBoundCountsSharedTimestampsNotTheSpannedInterval) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t secondNs = 1000000000ULL;
        const uint64_t baseNs = 1704067200000000000ULL;
        constexpr size_t kBand = 100;
        const uint64_t bandStart = baseNs + 150000ULL * secondNs;

        // Shared: kBand one-second points in the middle.  Unshared: one point at
        // each end, 300,000 seconds apart -- so the interval CONTAINING the
        // intersection is 299,998 seconds wide while the intersection itself is
        // 99.
        std::vector<std::pair<uint64_t, double>> aPoints{{baseNs, 100.0}};
        std::vector<std::pair<uint64_t, double>> bPoints{{baseNs + secondNs, 2.0}};
        for (size_t i = 0; i < kBand; ++i) {
            aPoints.emplace_back(bandStart + i * secondNs, 100.0);
            bPoints.emplace_back(bandStart + i * secondNs, 2.0);
        }
        aPoints.emplace_back(baseNs + 300000ULL * secondNs, 100.0);
        bPoints.emplace_back(baseNs + 299999ULL * secondNs, 2.0);

        for (const std::string& device : {"DEV-A", "DEV-B"}) {
            insertFloatSeries(eng.eng, "bnd_a", "v", {{"deviceId", device}}, aPoints);
            insertFloatSeries(eng.eng, "bnd_b", "v", {{"deviceId", device}}, bPoints);
        }
        const uint64_t endNs = baseNs + 400000ULL * secondNs;

        DerivedQueryExecutor executor(&eng.eng);
        const std::string legA = "avg:bnd_a(v){} by {deviceId}";
        const std::string legB = "avg:bnd_b(v){} by {deviceId}";

        // GROUND TRUTH: one group is exempt from the bound, so it shows what the
        // shape really costs.
        auto truth = executor
                         .executeFromJson(derivedJsonBodyTwoLegsWithInterval(
                             "avg:bnd_a(v){deviceId:DEV-A} by {deviceId}", "avg:bnd_b(v){deviceId:DEV-A} by {deviceId}",
                             "a / b", baseNs, endNs, "1s"))
                         .get();
        ASSERT_EQ(truth.series.size(), 1u);
        ASSERT_EQ(truth.series[0].values.size(), kBand);

        // TWO groups: the bound applies, and must reach the same number.
        auto both =
            executor.executeFromJson(derivedJsonBodyTwoLegsWithInterval(legA, legB, "a / b", baseNs, endNs, "1s"))
                .get();
        ASSERT_EQ(both.series.size(), 2u) << "refused as ~300,000 points for an answer of " << 2 * kBand;
        for (const auto& group : both.series) {
            ASSERT_EQ(group.values.size(), kBand);
            EXPECT_DOUBLE_EQ(group.values[0], 50.0);
        }
        EXPECT_EQ(both.stats.pointCount, 2 * kBand);

        // ... and it is still the SAME answer the un-bucketed query gives, which
        // was never refused because no grid was projected for it.
        auto raw = executor.executeFromJson(derivedJsonBodyTwoLegs(legA, legB, "a / b", baseNs, endNs, true)).get();
        ASSERT_EQ(raw.series.size(), 2u);
        EXPECT_EQ(raw.series[0].values.size(), kBand);
    })
        .join()
        .get();
}

// ---- B2, the zero case: overlapping spans, no shared timestamp -------------
//
// Projected over the containing interval this claimed 299,999 points for a
// result of NONE.  The groups are still emitted (empty), because the caller
// asked for them by name.
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesBoundIsZeroWhenTheLegsShareNoTimestamp) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t secondNs = 1000000000ULL;
        const uint64_t baseNs = 1704067200000000000ULL;
        for (const std::string& device : {"DEV-A", "DEV-B"}) {
            insertFloatSeries(eng.eng, "dj_a", "v", {{"deviceId", device}},
                              {{baseNs, 100.0}, {baseNs + 300000ULL * secondNs, 100.0}});
            insertFloatSeries(eng.eng, "dj_b", "v", {{"deviceId", device}},
                              {{baseNs + secondNs, 2.0}, {baseNs + 299999ULL * secondNs, 2.0}});
        }
        const uint64_t endNs = baseNs + 400000ULL * secondNs;

        DerivedQueryExecutor executor(&eng.eng);
        auto result =
            executor
                .executeFromJson(derivedJsonBodyTwoLegsWithInterval(
                    "avg:dj_a(v){} by {deviceId}", "avg:dj_b(v){} by {deviceId}", "a / b", baseNs, endNs, "1s"))
                .get();

        ASSERT_EQ(result.series.size(), 2u) << "refused as ~300,000 points for an answer of none";
        for (const auto& group : result.series) {
            EXPECT_TRUE(group.values.empty());
        }
        EXPECT_EQ(result.stats.pointCount, 0u);
    })
        .join()
        .get();
}

// ---- B2: an over-large group is STILL refused, and now up front ------------
//
// Exactness must not be mistaken for permissiveness: a dense grid the legs
// really do span is refused exactly as before.
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesBoundStillRefusesAGridTheLegsReallySpan) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t secondNs = 1000000000ULL;
        const uint64_t baseNs = 1704067200000000000ULL;
        for (const std::string& device : {"DEV-A", "DEV-B"}) {
            for (const std::string& measurement : {"wide_a", "wide_b"}) {
                insertFloatSeries(eng.eng, measurement, "v", {{"deviceId", device}},
                                  {{baseNs, 10.0}, {baseNs + 300000ULL * secondNs, 20.0}});
            }
        }
        const uint64_t endNs = baseNs + 400000ULL * secondNs;

        DerivedQueryExecutor executor(&eng.eng);
        try {
            executor
                .executeFromJson(derivedJsonBodyTwoLegsWithInterval(
                    "avg:wide_a(v){} by {deviceId}", "avg:wide_b(v){} by {deviceId}", "a / b", baseNs, endNs, "1s"))
                .get();
            FAIL() << "Expected the fan-out point bound to refuse a 300,001-point grid per group";
        } catch (const DerivedQueryException& e) {
            const std::string msg = e.what();
            EXPECT_NE(msg.find("would produce at least"), std::string::npos)
                << "must be the PRE-check, not the after-the-fact backstop: " << msg;
            EXPECT_NE(msg.find("300001"), std::string::npos) << "must state the real size: " << msg;
        }
    })
        .join()
        .get();
}

// ============================================================================
// Phase 5: the remaining arms of the three-way FIELD broadcast rule
//
// fieldBroadcastsOverEveryGroup() admits a one-group leg's field when it is
// named by NO output key (the shared scalar denominator) or by ALL of them, and
// refuses it when it is named by SOME.  Before this phase only the SOME arm was
// pinned by a test that a mutation could kill: mutating the whole predicate to
// `true` killed exactly ONE test, so the entire rule rested on a single guard.
// ============================================================================

static std::string derivedJsonBodyThreeLegs(const std::string& legA, const std::string& legB, const std::string& legC,
                                            const std::string& formula, uint64_t startNs, uint64_t endNs) {
    return R"json({"queries":{"a":")json" + legA + R"json(","b":")json" + legB + R"json(","c":")json" + legC +
           R"json("},"formula":")json" + formula + R"json(","startTime":)json" + std::to_string(startNs) +
           R"json(,"endTime":)json" + std::to_string(endNs) + R"json(,"multiSeries":true})json";
}

// ---- the ALL arm: a field EVERY output group carries does broadcast --------
//
// The third arm of the rule, and the awkward one to observe: a SUCCESSFUL
// ALL-arm broadcast is not constructible.  The field only enters the pairing
// key when some leg resolves to more than one distinct field name, and such a
// leg necessarily resolves to more than one group -- so it is either the
// reference (and the output keys then span two fields, which no single group's
// field can all match) or it is checked against the reference and refused for a
// key-set mismatch.  Whenever the field is IN the key, the output keys span
// more than one field.
//
// The arm is still decisive, and this test is how: with legs a (two devices,
// field rx), b (one untagged group, field rx) and z (two fields, which is what
// puts the field in the key), the legs are checked in NAME order, so b is
// tested BEFORE z.  Under the rule as written b takes the ALL arm and the query
// fails on z's key-set mismatch.  Drop the ALL arm and b is refused first, and
// the caller is told to fix the wrong sub-query.
//
// So: assert WHICH leg the 400 blames.  That is the observable difference, and
// it is what makes `matched == outputKeys.size()` a tested guard rather than a
// dead one.
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesOneGroupLegWhoseFieldEveryOutputGroupCarriesBroadcasts) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 3;
        for (size_t i = 0; i < kPoints; ++i) {
            const uint64_t ts = startNs + i * minuteNs;
            insertFloatSeries(eng.eng, "allf_a", "rx", {{"dev", "DEV-A"}}, {{ts, 10.0}});
            insertFloatSeries(eng.eng, "allf_a", "rx", {{"dev", "DEV-B"}}, {{ts, 20.0}});
            insertFloatSeries(eng.eng, "allf_b", "rx", {}, {{ts, 2.0}});
            insertFloatSeries(eng.eng, "allf_z", "rx", {}, {{ts, 5.0}});
            insertFloatSeries(eng.eng, "allf_z", "tx", {}, {{ts, 7.0}});
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        try {
            auto wrong =
                executor
                    .executeFromJson(derivedJsonBodyThreeLegs("avg:allf_a(rx){} by {dev}", "avg:allf_b(rx){}",
                                                              "avg:allf_z(rx,tx){}", "a / b / c", startNs, endNs))
                    .get();
            ADD_FAILURE() << "Expected the key-set mismatch on 'c': instead got " << wrong.series.size() << " groups";
        } catch (const DerivedQueryException& e) {
            const std::string msg = e.what();
            EXPECT_NE(msg.find("'a' and 'c' resolved to different series"), std::string::npos)
                << "the two-field leg is the disagreement, and must be the one named: " << msg;
            // 'b' carries field rx, which EVERY output key carries, so it
            // broadcasts and is never blamed.  Without the ALL arm this is the
            // message the caller would get instead, naming the wrong leg.
            EXPECT_EQ(msg.find("Sub-query 'b'"), std::string::npos)
                << "'b' names a field every output group carries -- it broadcasts: " << msg;
            EXPECT_EQ(msg.find("carrying field"), std::string::npos)
                << "the field-broadcast refusal must not fire for 'b': " << msg;
        }

        // And with the two-field leg removed, the field leaves the pairing key
        // entirely (every leg names exactly one field) and the same broadcast is
        // simply the answer -- 10/2 and 20/2, per device.
        auto ok = executor
                      .executeFromJson(derivedJsonBodyTwoLegs("avg:allf_a(rx){} by {dev}", "avg:allf_b(rx){}", "a / b",
                                                              startNs, endNs, /*multiSeries=*/true))
                      .get();
        ASSERT_EQ(ok.series.size(), 2u);
        EXPECT_EQ(ok.series[0].groupTags, std::vector<std::string>{"dev=DEV-A"});
        EXPECT_EQ(ok.series[1].groupTags, std::vector<std::string>{"dev=DEV-B"});
        ASSERT_EQ(ok.series[0].values.size(), kPoints);
        ASSERT_EQ(ok.series[1].values.size(), kPoints);
        for (size_t i = 0; i < kPoints; ++i) {
            EXPECT_DOUBLE_EQ(ok.series[0].values[i], 5.0) << "DEV-A i=" << i;
            EXPECT_DOUBLE_EQ(ok.series[1].values[i], 10.0) << "DEV-B i=" << i;
        }
    })
        .join()
        .get();
}

// ---- with SEVERAL single-group legs, the 400 names the FAILING one ---------
//
// A three-leg formula where one single-group leg passes the field test (its
// field is named by no output key -- the scalar denominator) and another fails
// it (its field is named by some but not all).  The message must blame the leg
// that cannot broadcast, whichever NAME it happens to have: the legs are
// checked in name order, so the failing leg is reached second in one order and
// first in the other, and a message that named the leg it happened to be
// looking at, or the first single-group leg it saw, would be right only half
// the time.
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesFieldMismatchNamesTheFailingLegNotItsInnocentSibling) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 3;
        for (size_t i = 0; i < kPoints; ++i) {
            const uint64_t ts = startNs + i * minuteNs;
            insertFloatSeries(eng.eng, "tri_in", "rx", {{"deviceId", "DEV-A"}}, {{ts, 10.0}});
            insertFloatSeries(eng.eng, "tri_in", "tx", {{"deviceId", "DEV-A"}}, {{ts, 20.0}});
            insertFloatSeries(eng.eng, "tri_scalar", "bytes", {}, {{ts, 4.0}});
            insertFloatSeries(eng.eng, "tri_out", "rx", {}, {{ts, 2.0}});
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        const std::string scalarLeg = "avg:tri_scalar(bytes){}";  // broadcasts: named by NO output key
        const std::string rxLeg = "avg:tri_out(rx){}";            // refused: named by SOME but not all

        DerivedQueryExecutor executor(&eng.eng);
        // {which name carries the failing leg, legB, legC}
        const std::vector<std::tuple<std::string, std::string, std::string>> orders = {
            {"c", scalarLeg, rxLeg},  // failing leg checked SECOND
            {"b", rxLeg, scalarLeg},  // failing leg checked FIRST
        };

        for (const auto& [failing, legB, legC] : orders) {
            const std::string innocent = (failing == "b") ? "c" : "b";
            try {
                auto wrong = executor
                                 .executeFromJson(derivedJsonBodyThreeLegs("avg:tri_in(rx,tx){} by {deviceId}", legB,
                                                                           legC, "a / b / c", startNs, endNs))
                                 .get();
                ADD_FAILURE() << "Expected a 400 naming '" << failing << "': instead got " << wrong.series.size()
                              << " groups";
            } catch (const DerivedQueryException& e) {
                const std::string msg = e.what();
                EXPECT_NE(msg.find("Sub-query '" + failing + "' resolved to a single series carrying field 'rx'"),
                          std::string::npos)
                    << "must blame the leg that cannot broadcast: " << msg;
                EXPECT_EQ(msg.find("Sub-query '" + innocent + "'"), std::string::npos)
                    << "the scalar denominator broadcasts and must not be blamed: " << msg;
                EXPECT_NE(msg.find("some but not all"), std::string::npos) << "must state the rule: " << msg;
            }
        }
    })
        .join()
        .get();
}

// ---- a leg failing BOTH axes is reported as the TAG mismatch --------------
//
// Tags are tested first and their message wins, deliberately: the two remedies
// differ (widen the scope filter vs name the same fields), and a leg whose scope
// is too narrow on the tag axis will usually stop failing on the field axis too
// once the scope is fixed.  Reporting both, or the field one, would send the
// caller after the second-order problem.
TEST_F(DerivedQueryExecutorSeastarTest, MultiSeriesLegFailingBothAxesReportsTheTagMismatch) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        const uint64_t startNs = 1704067200000000000ULL;
        const uint64_t minuteNs = 60000000000ULL;
        constexpr size_t kPoints = 3;
        for (size_t i = 0; i < kPoints; ++i) {
            const uint64_t ts = startNs + i * minuteNs;
            insertFloatSeries(eng.eng, "both_in", "rx", {{"deviceId", "DEV-A"}}, {{ts, 10.0}});
            insertFloatSeries(eng.eng, "both_in", "tx", {{"deviceId", "DEV-A"}}, {{ts, 20.0}});
            insertFloatSeries(eng.eng, "both_in", "rx", {{"deviceId", "DEV-B"}}, {{ts, 11.0}});
            insertFloatSeries(eng.eng, "both_in", "tx", {{"deviceId", "DEV-B"}}, {{ts, 21.0}});
            insertFloatSeries(eng.eng, "both_out", "rx", {{"deviceId", "DEV-A"}}, {{ts, 2.0}});
        }
        const uint64_t endNs = startNs + kPoints * minuteNs;

        DerivedQueryExecutor executor(&eng.eng);
        try {
            // b is tagged deviceId=DEV-A (not a subset of DEV-B's groups) AND
            // carries field rx (named by two of the four output keys), so it
            // fails the subset test and the field test alike.
            auto wrong = executor
                             .executeFromJson(derivedJsonBodyTwoLegs("avg:both_in(rx,tx){} by {deviceId}",
                                                                     "avg:both_out(rx){deviceId:DEV-A} by {deviceId}",
                                                                     "a / b", startNs, endNs, /*multiSeries=*/true))
                             .get();
            ADD_FAILURE() << "Expected a 400: instead got " << wrong.series.size() << " groups";
        } catch (const DerivedQueryException& e) {
            const std::string msg = e.what();
            EXPECT_NE(msg.find("single series tagged (deviceId=DEV-A)"), std::string::npos)
                << "tags are tested first and their message wins: " << msg;
            EXPECT_NE(msg.find("Widen this sub-query's scope"), std::string::npos)
                << "must give the TAG remedy: " << msg;
            EXPECT_EQ(msg.find("carrying field"), std::string::npos)
                << "the field message must not also be raised -- one cause, one remedy: " << msg;
            EXPECT_EQ(msg.find("some but not all"), std::string::npos) << "field rule must not be quoted: " << msg;
        }
    })
        .join()
        .get();
}
