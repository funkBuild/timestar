// Seastar-based async tests for ForecastExecutor
// Tests multi-series async execution through the DerivedQueryExecutor path,
// large dataset handling, different algorithms, and error propagation.
// The ForecastExecutor itself is synchronous, but it is invoked asynchronously
// via DerivedQueryExecutor::executeForecast() which reads from a real engine.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/query/derived_query.hpp"
#include "../../../lib/query/derived_query_executor.hpp"
#include "../../../lib/query/forecast/forecast_executor.hpp"
#include "../../../lib/query/forecast/forecast_result.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <filesystem>
#include <random>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <string>
#include <vector>

namespace fs = std::filesystem;

using namespace timestar;
using namespace timestar::forecast;

class ForecastExecutorSeastarTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }

    void TearDown() override { cleanTestShardDirectories(); }

    // Insert a series of float data points via shardedInsert
    static void insertSeries(seastar::sharded<Engine>& eng, const std::string& measurement, const std::string& field,
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
};

// ===========================================================================
// 1. Linear forecast through DerivedQueryExecutor async path
// ===========================================================================

TEST_F(ForecastExecutorSeastarTest, LinearForecastViaExecutor) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert 100 points of linear trend: y = 10 + 0.5*x
        uint64_t intervalNs = 60000000000ULL;
        uint64_t startNs = 1704067200000000000ULL;
        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < 100; ++i) {
            points.push_back({startNs + i * intervalNs, 10.0 + 0.5 * i});
        }
        insertSeries(eng.eng, "temp", "value", {{"loc", "east"}}, points);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "forecast(a, 'linear', 2)";
        request.startTime = startNs;
        request.endTime = startNs + 200 * intervalNs;

        QueryRequest qr;
        qr.measurement = "temp";
        qr.fields = {"value"};
        qr.scopes = {{"loc", "east"}};
        qr.startTime = startNs;
        qr.endTime = startNs + 100 * intervalNs;
        request.queries["a"] = qr;

        auto variantResult = executor.executeWithAnomaly(request).get();

        ASSERT_TRUE(std::holds_alternative<ForecastQueryResult>(variantResult));
        auto& result = std::get<ForecastQueryResult>(variantResult);

        EXPECT_TRUE(result.success);
        EXPECT_FALSE(result.empty());

        // Should have past, forecast, upper, lower pieces
        EXPECT_EQ(result.series.size(), 4u);

        // Check piece types
        const ForecastSeriesPiece* past = result.getPiece("past");
        const ForecastSeriesPiece* fcst = result.getPiece("forecast");
        const ForecastSeriesPiece* upper = result.getPiece("upper");
        const ForecastSeriesPiece* lower = result.getPiece("lower");

        EXPECT_NE(past, nullptr);
        EXPECT_NE(fcst, nullptr);
        EXPECT_NE(upper, nullptr);
        EXPECT_NE(lower, nullptr);

        // Validate statistics
        EXPECT_EQ(result.statistics.algorithm, "linear");
        EXPECT_GT(result.statistics.historicalPoints, 0u);
        EXPECT_GT(result.statistics.forecastPoints, 0u);

        // Slope should be close to 0.5
        EXPECT_NEAR(result.statistics.slope, 0.5, 0.1);
    })
        .join()
        .get();
}

// ===========================================================================
// 2. Seasonal forecast through async path
// ===========================================================================

TEST_F(ForecastExecutorSeastarTest, SeasonalForecastViaExecutor) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert sinusoidal data
        uint64_t intervalNs = 60000000000ULL;
        uint64_t startNs = 1704067200000000000ULL;
        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < 100; ++i) {
            double val = 100.0 + 20.0 * std::sin(2.0 * M_PI * i / 24);
            points.push_back({startNs + i * intervalNs, val});
        }
        insertSeries(eng.eng, "temp", "value", {{"loc", "west"}}, points);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "forecast(a, 'seasonal', 2)";
        request.startTime = startNs;
        request.endTime = startNs + 200 * intervalNs;

        QueryRequest qr;
        qr.measurement = "temp";
        qr.fields = {"value"};
        qr.scopes = {{"loc", "west"}};
        qr.startTime = startNs;
        qr.endTime = startNs + 100 * intervalNs;
        request.queries["a"] = qr;

        auto variantResult = executor.executeWithAnomaly(request).get();

        ASSERT_TRUE(std::holds_alternative<ForecastQueryResult>(variantResult));
        auto& result = std::get<ForecastQueryResult>(variantResult);

        EXPECT_TRUE(result.success);
        EXPECT_FALSE(result.empty());
        EXPECT_EQ(result.statistics.algorithm, "seasonal");
        EXPECT_GE(result.series.size(), 4u);
    })
        .join()
        .get();
}

// ===========================================================================
// 3. Large dataset forecast (1000 points)
// ===========================================================================

TEST_F(ForecastExecutorSeastarTest, LargeDatasetForecast) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert 1000 noisy linear points
        uint64_t intervalNs = 60000000000ULL;
        uint64_t startNs = 1704067200000000000ULL;
        std::mt19937 gen(42);
        std::normal_distribution<> noise(0.0, 2.0);

        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < 1000; ++i) {
            double val = 50.0 + 0.1 * i + noise(gen);
            points.push_back({startNs + i * intervalNs, val});
        }
        insertSeries(eng.eng, "sensor", "reading", {{"id", "s1"}}, points);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "forecast(a, 'linear', 2)";
        request.startTime = startNs;
        request.endTime = startNs + 1500 * intervalNs;

        QueryRequest qr;
        qr.measurement = "sensor";
        qr.fields = {"reading"};
        qr.scopes = {{"id", "s1"}};
        qr.startTime = startNs;
        qr.endTime = startNs + 1000 * intervalNs;
        request.queries["a"] = qr;

        auto variantResult = executor.executeWithAnomaly(request).get();

        ASSERT_TRUE(std::holds_alternative<ForecastQueryResult>(variantResult));
        auto& result = std::get<ForecastQueryResult>(variantResult);

        EXPECT_TRUE(result.success);
        EXPECT_FALSE(result.empty());

        // Verify execution completed in reasonable time
        EXPECT_GT(result.statistics.executionTimeMs, 0.0);
        EXPECT_LT(result.statistics.executionTimeMs, 30000.0);  // under 30s

        // Should have forecast points
        EXPECT_GT(result.statistics.forecastPoints, 0u);
        EXPECT_EQ(result.statistics.historicalPoints, 1000u);
    })
        .join()
        .get();
}

// ===========================================================================
// 4. Forecast timestamp generation sanity check
// ===========================================================================

TEST_F(ForecastExecutorSeastarTest, ForecastTimestampGeneration) {
    // This test doesn't need an engine -- just verifies the static method
    uint64_t intervalNs = 60000000000ULL;
    uint64_t startNs = 1704067200000000000ULL;

    std::vector<uint64_t> timestamps;
    for (size_t i = 0; i < 10; ++i) {
        timestamps.push_back(startNs + i * intervalNs);
    }

    auto forecastTs = ForecastExecutor::generateForecastTimestamps(timestamps, 5);

    ASSERT_EQ(forecastTs.size(), 5u);
    EXPECT_GT(forecastTs[0], timestamps.back());

    // Verify uniform spacing
    uint64_t expectedInterval = intervalNs;
    for (size_t i = 1; i < forecastTs.size(); ++i) {
        EXPECT_EQ(forecastTs[i] - forecastTs[i - 1], expectedInterval);
    }
}

// ===========================================================================
// 5. Forecast with insufficient data returns empty
// ===========================================================================

TEST_F(ForecastExecutorSeastarTest, SmallDatasetDoesNotCrash) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert only 3 points -- may or may not be enough depending on minDataPoints
        uint64_t intervalNs = 60000000000ULL;
        uint64_t startNs = 1704067200000000000ULL;
        std::vector<std::pair<uint64_t, double>> points = {
            {startNs, 1.0}, {startNs + intervalNs, 2.0}, {startNs + 2 * intervalNs, 3.0}};
        insertSeries(eng.eng, "metric", "val", {}, points);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "forecast(a, 'linear', 2)";
        request.startTime = startNs;
        request.endTime = startNs + 10 * intervalNs;

        QueryRequest qr;
        qr.measurement = "metric";
        qr.fields = {"val"};
        qr.startTime = startNs;
        qr.endTime = startNs + 3 * intervalNs;
        request.queries["a"] = qr;

        // The important thing is it doesn't crash or throw
        EXPECT_NO_THROW({
            auto variantResult = executor.executeWithAnomaly(request).get();

            ASSERT_TRUE(std::holds_alternative<ForecastQueryResult>(variantResult));
            auto& result = std::get<ForecastQueryResult>(variantResult);

            // With only 3 data points (below default minDataPoints=10),
            // the forecaster will not produce series output but should
            // still complete successfully without crashing.
            EXPECT_TRUE(result.success);
        });
    })
        .join()
        .get();
}

// ===========================================================================
// 6. Forecast JSON response formatting
// ===========================================================================

TEST_F(ForecastExecutorSeastarTest, ForecastResponseFormatting) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert data
        uint64_t intervalNs = 60000000000ULL;
        uint64_t startNs = 1704067200000000000ULL;
        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < 50; ++i) {
            points.push_back({startNs + i * intervalNs, 100.0 + i * 0.5});
        }
        insertSeries(eng.eng, "metric", "val", {{"zone", "a"}}, points);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "forecast(a, 'linear', 2)";
        request.startTime = startNs;
        request.endTime = startNs + 100 * intervalNs;

        QueryRequest qr;
        qr.measurement = "metric";
        qr.fields = {"val"};
        qr.scopes = {{"zone", "a"}};
        qr.startTime = startNs;
        qr.endTime = startNs + 50 * intervalNs;
        request.queries["a"] = qr;

        auto variantResult = executor.executeWithAnomaly(request).get();
        ASSERT_TRUE(std::holds_alternative<ForecastQueryResult>(variantResult));
        auto& result = std::get<ForecastQueryResult>(variantResult);

        // Format the response
        auto json = executor.formatForecastResponse(result);

        // Validate JSON structure
        EXPECT_TRUE(json.find("\"success\"") != std::string::npos);
        EXPECT_TRUE(json.find("\"times\"") != std::string::npos);
        EXPECT_TRUE(json.find("\"forecast_start_index\"") != std::string::npos);
        EXPECT_TRUE(json.find("\"series\"") != std::string::npos);
        EXPECT_TRUE(json.find("\"linear\"") != std::string::npos);

        // Also test via formatResponseVariant
        DerivedQueryResultVariant variant{result};
        auto json2 = executor.formatResponseVariant(variant);
        EXPECT_TRUE(json2.find("\"success\"") != std::string::npos);
    })
        .join()
        .get();
}

// ===========================================================================
// 7. Forecast with data rollover to TSM
// ===========================================================================

TEST_F(ForecastExecutorSeastarTest, ForecastAfterRolloverToTSM) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        uint64_t intervalNs = 60000000000ULL;
        uint64_t startNs = 1704067200000000000ULL;

        // Insert data in two batches with rollover
        std::vector<std::pair<uint64_t, double>> batch1;
        for (size_t i = 0; i < 50; ++i) {
            batch1.push_back({startNs + i * intervalNs, 10.0 + i * 0.2});
        }
        insertSeries(eng.eng, "metric", "val", {{"host", "h1"}}, batch1);

        // Force rollover to TSM
        eng.eng.invoke_on_all([](Engine& engine) { return engine.rolloverMemoryStore(); }).get();

        // Insert second batch (stays in memory)
        std::vector<std::pair<uint64_t, double>> batch2;
        for (size_t i = 50; i < 100; ++i) {
            batch2.push_back({startNs + i * intervalNs, 10.0 + i * 0.2});
        }
        insertSeries(eng.eng, "metric", "val", {{"host", "h1"}}, batch2);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "forecast(a, 'linear', 2)";
        request.startTime = startNs;
        request.endTime = startNs + 200 * intervalNs;

        QueryRequest qr;
        qr.measurement = "metric";
        qr.fields = {"val"};
        qr.scopes = {{"host", "h1"}};
        qr.startTime = startNs;
        qr.endTime = startNs + 100 * intervalNs;
        request.queries["a"] = qr;

        auto variantResult = executor.executeWithAnomaly(request).get();

        ASSERT_TRUE(std::holds_alternative<ForecastQueryResult>(variantResult));
        auto& result = std::get<ForecastQueryResult>(variantResult);

        EXPECT_TRUE(result.success);
        EXPECT_FALSE(result.empty());

        // The merged data from TSM + memory should produce a linear trend
        EXPECT_GT(result.statistics.historicalPoints, 0u);
        EXPECT_NEAR(result.statistics.slope, 0.2, 0.05);
    })
        .join()
        .get();
}

// ===========================================================================
// 8. ForecastExecutor multi-series direct test (no engine needed)
// ===========================================================================

TEST_F(ForecastExecutorSeastarTest, MultiSeriesDirectExecution) {
    // Direct test of ForecastExecutor::executeMulti (synchronous)
    // Verifies multi-series result aggregation

    uint64_t intervalNs = 60000000000ULL;
    uint64_t startNs = 1704067200000000000ULL;

    std::vector<uint64_t> timestamps;
    for (size_t i = 0; i < 50; ++i) {
        timestamps.push_back(startNs + i * intervalNs);
    }

    // Two series with different slopes
    std::vector<std::vector<double>> seriesValues(2);
    for (size_t i = 0; i < 50; ++i) {
        seriesValues[0].push_back(10.0 + 0.5 * i);   // slope 0.5
        seriesValues[1].push_back(100.0 + 1.0 * i);  // slope 1.0
    }

    std::vector<std::vector<std::string>> seriesGroupTags = {{"host=server1"}, {"host=server2"}};

    ForecastConfig config;
    config.algorithm = Algorithm::LINEAR;
    config.deviations = 2.0;
    config.forecastHorizon = 10;

    ForecastExecutor executor;
    auto result = executor.executeMulti(timestamps, seriesValues, seriesGroupTags, config);

    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.statistics.seriesCount, 2u);

    // 4 pieces per series = 8 total
    EXPECT_EQ(result.series.size(), 8u);

    // Check piece types
    int pastCount = 0, forecastCount = 0, upperCount = 0, lowerCount = 0;
    for (const auto& piece : result.series) {
        if (piece.piece == "past")
            pastCount++;
        else if (piece.piece == "forecast")
            forecastCount++;
        else if (piece.piece == "upper")
            upperCount++;
        else if (piece.piece == "lower")
            lowerCount++;
    }
    EXPECT_EQ(pastCount, 2);
    EXPECT_EQ(forecastCount, 2);
    EXPECT_EQ(upperCount, 2);
    EXPECT_EQ(lowerCount, 2);

    // Verify forecast start index
    EXPECT_EQ(result.forecastStartIndex, 50u);

    // Verify timestamps include historical + forecast
    EXPECT_EQ(result.times.size(), 60u);  // 50 historical + 10 forecast
}
