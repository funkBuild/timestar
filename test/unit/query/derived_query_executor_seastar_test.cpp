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
