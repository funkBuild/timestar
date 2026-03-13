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

#include <cmath>
#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
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
        DerivedQueryExecutor executor(&eng.eng, nullptr, config);

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
// 18. isAnomalyFormula / isForecastFormula static helpers
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
