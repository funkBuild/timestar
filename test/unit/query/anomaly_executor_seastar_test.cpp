// Seastar-based async tests for AnomalyExecutor
// Tests multi-series async execution through the DerivedQueryExecutor path,
// result aggregation, alert computation, large datasets, and different
// anomaly algorithms. The AnomalyExecutor itself is synchronous, but it is
// invoked asynchronously via DerivedQueryExecutor::executeAnomalyDetection().

#include <gtest/gtest.h>
#include <filesystem>
#include <vector>
#include <string>
#include <cmath>
#include <random>

#include "../../../lib/query/derived_query_executor.hpp"
#include "../../../lib/query/derived_query.hpp"
#include "../../../lib/query/anomaly/anomaly_executor.hpp"
#include "../../../lib/query/anomaly/anomaly_result.hpp"
#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/tsdb_value.hpp"
#include "../../../lib/core/series_id.hpp"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include "../../seastar_gtest.hpp"
#include "../../test_helpers.hpp"

namespace fs = std::filesystem;

using namespace tsdb;
using namespace tsdb::anomaly;

class AnomalyExecutorSeastarTest : public ::testing::Test {
protected:
    void SetUp() override {
        cleanTestShardDirectories();
    }

    void TearDown() override {
        cleanTestShardDirectories();
    }

    static void insertSeries(seastar::sharded<Engine>& eng,
                             const std::string& measurement,
                             const std::string& field,
                             const std::map<std::string, std::string>& tags,
                             const std::vector<std::pair<uint64_t, double>>& points) {
        TSDBInsert<double> insert(measurement, field);
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
// 1. Basic anomaly detection through DerivedQueryExecutor async path
// ===========================================================================

TEST_F(AnomalyExecutorSeastarTest, BasicAnomalyViaExecutor) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert 100 stable points with one outlier
        uint64_t intervalNs = 60000000000ULL;
        uint64_t startNs = 1704067200000000000ULL;
        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < 100; ++i) {
            double val = 50.0;
            if (i == 75) val = 200.0; // obvious anomaly
            points.push_back({startNs + i * intervalNs, val});
        }
        insertSeries(eng.eng, "cpu", "usage", {{"host", "s1"}}, points);

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

        ASSERT_TRUE(std::holds_alternative<AnomalyQueryResult>(variantResult));
        auto& result = std::get<AnomalyQueryResult>(variantResult);

        EXPECT_TRUE(result.success);
        EXPECT_EQ(result.times.size(), 100u);

        // Verify all expected pieces are present
        EXPECT_NE(result.getPiece("raw"), nullptr);
        EXPECT_NE(result.getPiece("upper"), nullptr);
        EXPECT_NE(result.getPiece("lower"), nullptr);
        EXPECT_NE(result.getPiece("scores"), nullptr);

        // Raw values should match inserted data
        const auto* raw = result.getPiece("raw");
        ASSERT_NE(raw, nullptr);
        EXPECT_EQ(raw->values.size(), 100u);
        EXPECT_DOUBLE_EQ(raw->values[0], 50.0);
        EXPECT_DOUBLE_EQ(raw->values[75], 200.0);

        // Statistics
        EXPECT_EQ(result.statistics.algorithm, "basic");
        EXPECT_DOUBLE_EQ(result.statistics.bounds, 2.0);
        EXPECT_EQ(result.statistics.totalPoints, 100u);
        EXPECT_GE(result.statistics.anomalyCount, 1u);
    }).join().get();
}

// ===========================================================================
// 2. Agile algorithm through async path
// ===========================================================================

TEST_F(AnomalyExecutorSeastarTest, AgileAlgorithmViaExecutor) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert data with a level shift
        uint64_t intervalNs = 60000000000ULL;
        uint64_t startNs = 1704067200000000000ULL;
        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < 200; ++i) {
            double val = (i < 100) ? 50.0 : 100.0; // level shift at i=100
            points.push_back({startNs + i * intervalNs, val});
        }
        insertSeries(eng.eng, "load", "avg", {{"dc", "us"}}, points);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "anomalies(a, 'agile', 2)";
        request.startTime = startNs;
        request.endTime = startNs + 200 * intervalNs;

        QueryRequest qr;
        qr.measurement = "load";
        qr.fields = {"avg"};
        qr.scopes = {{"dc", "us"}};
        qr.startTime = request.startTime;
        qr.endTime = request.endTime;
        request.queries["a"] = qr;

        auto variantResult = executor.executeWithAnomaly(request).get();

        ASSERT_TRUE(std::holds_alternative<AnomalyQueryResult>(variantResult));
        auto& result = std::get<AnomalyQueryResult>(variantResult);

        EXPECT_TRUE(result.success);
        EXPECT_EQ(result.times.size(), 200u);
        EXPECT_EQ(result.statistics.algorithm, "agile");
        EXPECT_GE(result.series.size(), 4u);
    }).join().get();
}

// ===========================================================================
// 3. Robust algorithm through async path
// ===========================================================================

TEST_F(AnomalyExecutorSeastarTest, RobustAlgorithmViaExecutor) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert constant data with an outlier
        uint64_t intervalNs = 60000000000ULL;
        uint64_t startNs = 1704067200000000000ULL;
        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < 100; ++i) {
            double val = 80.0;
            if (i == 60) val = 300.0;
            points.push_back({startNs + i * intervalNs, val});
        }
        insertSeries(eng.eng, "net", "bytes", {{"iface", "eth0"}}, points);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "anomalies(a, 'robust', 3)";
        request.startTime = startNs;
        request.endTime = startNs + 100 * intervalNs;

        QueryRequest qr;
        qr.measurement = "net";
        qr.fields = {"bytes"};
        qr.scopes = {{"iface", "eth0"}};
        qr.startTime = request.startTime;
        qr.endTime = request.endTime;
        request.queries["a"] = qr;

        auto variantResult = executor.executeWithAnomaly(request).get();

        ASSERT_TRUE(std::holds_alternative<AnomalyQueryResult>(variantResult));
        auto& result = std::get<AnomalyQueryResult>(variantResult);

        EXPECT_TRUE(result.success);
        EXPECT_EQ(result.statistics.algorithm, "robust");
        EXPECT_DOUBLE_EQ(result.statistics.bounds, 3.0);
        EXPECT_GE(result.series.size(), 4u);
    }).join().get();
}

// ===========================================================================
// 4. Alert value computation
// ===========================================================================

TEST_F(AnomalyExecutorSeastarTest, AlertValueComputed) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert data with a known outlier
        uint64_t intervalNs = 60000000000ULL;
        uint64_t startNs = 1704067200000000000ULL;
        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < 100; ++i) {
            double val = 50.0;
            if (i == 80) val = 500.0; // large outlier
            points.push_back({startNs + i * intervalNs, val});
        }
        insertSeries(eng.eng, "metric", "val", {}, points);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "anomalies(a, 'basic', 2)";
        request.startTime = startNs;
        request.endTime = startNs + 100 * intervalNs;

        QueryRequest qr;
        qr.measurement = "metric";
        qr.fields = {"val"};
        qr.startTime = request.startTime;
        qr.endTime = request.endTime;
        request.queries["a"] = qr;

        auto variantResult = executor.executeWithAnomaly(request).get();

        ASSERT_TRUE(std::holds_alternative<AnomalyQueryResult>(variantResult));
        auto& result = std::get<AnomalyQueryResult>(variantResult);

        EXPECT_TRUE(result.success);

        // The "scores" piece should have an alert_value (maximum score)
        const auto* scores = result.getPiece("scores");
        ASSERT_NE(scores, nullptr);
        ASSERT_TRUE(scores->alertValue.has_value());
        EXPECT_GT(scores->alertValue.value(), 0.0);
    }).join().get();
}

// ===========================================================================
// 5. Large dataset anomaly detection (1000 points)
// ===========================================================================

TEST_F(AnomalyExecutorSeastarTest, LargeDatasetAnomalyDetection) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        // Insert 1000 noisy points with scattered anomalies
        uint64_t intervalNs = 60000000000ULL;
        uint64_t startNs = 1704067200000000000ULL;
        std::mt19937 gen(42);
        std::normal_distribution<> noise(0.0, 2.0);

        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < 1000; ++i) {
            double val = 100.0 + noise(gen);
            if (i == 200 || i == 500 || i == 800) {
                val += 100.0; // inject anomalies
            }
            points.push_back({startNs + i * intervalNs, val});
        }
        insertSeries(eng.eng, "sensor", "reading", {{"id", "s1"}}, points);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "anomalies(a, 'basic', 3)";
        request.startTime = startNs;
        request.endTime = startNs + 1000 * intervalNs;

        QueryRequest qr;
        qr.measurement = "sensor";
        qr.fields = {"reading"};
        qr.scopes = {{"id", "s1"}};
        qr.startTime = request.startTime;
        qr.endTime = request.endTime;
        request.queries["a"] = qr;

        auto variantResult = executor.executeWithAnomaly(request).get();

        ASSERT_TRUE(std::holds_alternative<AnomalyQueryResult>(variantResult));
        auto& result = std::get<AnomalyQueryResult>(variantResult);

        EXPECT_TRUE(result.success);
        EXPECT_EQ(result.times.size(), 1000u);
        EXPECT_EQ(result.statistics.totalPoints, 1000u);

        // Should detect at least some of the injected anomalies
        EXPECT_GE(result.statistics.anomalyCount, 1u);

        // Execution time should be reasonable
        EXPECT_GT(result.statistics.executionTimeMs, 0.0);
        EXPECT_LT(result.statistics.executionTimeMs, 30000.0);
    }).join().get();
}

// ===========================================================================
// 6. Anomaly JSON response formatting
// ===========================================================================

TEST_F(AnomalyExecutorSeastarTest, AnomalyResponseFormatting) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        uint64_t intervalNs = 60000000000ULL;
        uint64_t startNs = 1704067200000000000ULL;
        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < 100; ++i) {
            points.push_back({startNs + i * intervalNs, 50.0});
        }
        insertSeries(eng.eng, "metric", "val", {{"tag", "v1"}}, points);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "anomalies(a, 'basic', 2)";
        request.startTime = startNs;
        request.endTime = startNs + 100 * intervalNs;

        QueryRequest qr;
        qr.measurement = "metric";
        qr.fields = {"val"};
        qr.scopes = {{"tag", "v1"}};
        qr.startTime = request.startTime;
        qr.endTime = request.endTime;
        request.queries["a"] = qr;

        auto variantResult = executor.executeWithAnomaly(request).get();
        ASSERT_TRUE(std::holds_alternative<AnomalyQueryResult>(variantResult));
        auto& anomalyResult = std::get<AnomalyQueryResult>(variantResult);

        auto json = executor.formatAnomalyResponse(anomalyResult);

        // Validate JSON contains expected fields
        EXPECT_TRUE(json.find("\"success\"") != std::string::npos);
        EXPECT_TRUE(json.find("\"times\"") != std::string::npos);
        EXPECT_TRUE(json.find("\"series\"") != std::string::npos);
        EXPECT_TRUE(json.find("\"basic\"") != std::string::npos);
        EXPECT_TRUE(json.find("\"bounds\"") != std::string::npos);

        // Also verify via formatResponseVariant
        DerivedQueryResultVariant variant{anomalyResult};
        auto json2 = executor.formatResponseVariant(variant);
        EXPECT_TRUE(json2.find("\"success\"") != std::string::npos);
    }).join().get();
}

// ===========================================================================
// 7. Anomaly detection after TSM rollover
// ===========================================================================

TEST_F(AnomalyExecutorSeastarTest, AnomalyAfterRolloverToTSM) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        uint64_t intervalNs = 60000000000ULL;
        uint64_t startNs = 1704067200000000000ULL;

        // Batch 1: insert and rollover
        std::vector<std::pair<uint64_t, double>> batch1;
        for (size_t i = 0; i < 50; ++i) {
            batch1.push_back({startNs + i * intervalNs, 50.0});
        }
        insertSeries(eng.eng, "metric", "val", {{"host", "h1"}}, batch1);

        eng.eng.invoke_on_all([](Engine& engine) {
            return engine.rolloverMemoryStore();
        }).get();

        // Batch 2: stays in memory with an anomaly
        std::vector<std::pair<uint64_t, double>> batch2;
        for (size_t i = 50; i < 100; ++i) {
            double val = 50.0;
            if (i == 75) val = 300.0;
            batch2.push_back({startNs + i * intervalNs, val});
        }
        insertSeries(eng.eng, "metric", "val", {{"host", "h1"}}, batch2);

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "anomalies(a, 'basic', 2)";
        request.startTime = startNs;
        request.endTime = startNs + 100 * intervalNs;

        QueryRequest qr;
        qr.measurement = "metric";
        qr.fields = {"val"};
        qr.scopes = {{"host", "h1"}};
        qr.startTime = request.startTime;
        qr.endTime = request.endTime;
        request.queries["a"] = qr;

        auto variantResult = executor.executeWithAnomaly(request).get();

        ASSERT_TRUE(std::holds_alternative<AnomalyQueryResult>(variantResult));
        auto& result = std::get<AnomalyQueryResult>(variantResult);

        EXPECT_TRUE(result.success);
        EXPECT_EQ(result.times.size(), 100u);
        EXPECT_GE(result.statistics.anomalyCount, 1u);
    }).join().get();
}

// ===========================================================================
// 8. AnomalyExecutor multi-series direct test (no engine needed)
// ===========================================================================

TEST_F(AnomalyExecutorSeastarTest, MultiSeriesDirectExecution) {
    // Direct test of AnomalyExecutor::executeMulti (synchronous)

    uint64_t intervalNs = 60000000000ULL;
    uint64_t startNs = 1704067200000000000ULL;

    std::vector<uint64_t> timestamps;
    for (size_t i = 0; i < 100; ++i) {
        timestamps.push_back(startNs + i * intervalNs);
    }

    // Two series, one with anomaly
    std::vector<std::vector<double>> seriesValues(2);
    for (size_t i = 0; i < 100; ++i) {
        seriesValues[0].push_back(50.0);
        seriesValues[1].push_back(50.0);
    }
    seriesValues[0][60] = 300.0; // anomaly in first series
    seriesValues[1][80] = 300.0; // anomaly in second series

    std::vector<std::vector<std::string>> seriesGroupTags = {
        {"host=server1"},
        {"host=server2"}
    };

    AnomalyConfig config;
    config.algorithm = Algorithm::BASIC;
    config.bounds = 2.0;

    AnomalyExecutor executor;
    auto result = executor.executeMulti(timestamps, seriesValues, seriesGroupTags, config);

    EXPECT_TRUE(result.success);
    EXPECT_EQ(result.times.size(), 100u);

    // Should have at least 4 pieces per series (raw, upper, lower, scores) = 8+
    EXPECT_GE(result.series.size(), 8u);

    // Statistics
    EXPECT_EQ(result.statistics.algorithm, "basic");
    EXPECT_EQ(result.statistics.totalPoints, 200u);
    EXPECT_GE(result.statistics.anomalyCount, 2u);
}

// ===========================================================================
// 9. Anomaly with NaN/Inf in response formatting
// ===========================================================================

TEST_F(AnomalyExecutorSeastarTest, NaNInfResponseHandling) {
    // Test that NaN/Inf values in anomaly results are replaced with 0.0 in JSON

    DerivedQueryExecutor executor(nullptr);

    AnomalyQueryResult result;
    result.success = true;
    result.times = {1000, 2000, 3000};
    result.statistics.algorithm = "basic";
    result.statistics.bounds = 2.0;
    result.statistics.totalPoints = 3;

    // Create a series piece with NaN/Inf values
    AnomalySeriesPiece piece;
    piece.piece = "scores";
    piece.values = {1.0, std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::infinity()};
    piece.alertValue = 1.0;
    result.series.push_back(std::move(piece));

    auto json = executor.formatAnomalyResponse(result);

    // Should produce valid JSON without NaN/Inf literals
    EXPECT_TRUE(json.find("\"success\"") != std::string::npos);
    EXPECT_TRUE(json.find("NaN") == std::string::npos);
    EXPECT_TRUE(json.find("Infinity") == std::string::npos);
}

// ===========================================================================
// 10. Anomaly detection with empty result returns gracefully
// ===========================================================================

TEST_F(AnomalyExecutorSeastarTest, EmptyResultHandling) {
    AnomalyExecutor executor;

    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    std::vector<std::string> groupTags;

    AnomalyConfig config;
    config.algorithm = Algorithm::BASIC;
    config.bounds = 2.0;

    auto result = executor.execute(timestamps, values, groupTags, config);

    EXPECT_TRUE(result.success);
    EXPECT_TRUE(result.empty());
    EXPECT_EQ(result.series.size(), 0u);
}

// ===========================================================================
// 11. Anomaly detection preserves group tags
// ===========================================================================

TEST_F(AnomalyExecutorSeastarTest, GroupTagsPreserved) {
    AnomalyExecutor executor;

    uint64_t intervalNs = 60000000000ULL;
    uint64_t startNs = 1704067200000000000ULL;

    std::vector<uint64_t> timestamps;
    for (size_t i = 0; i < 100; ++i) {
        timestamps.push_back(startNs + i * intervalNs);
    }

    std::vector<double> values(100, 50.0);
    std::vector<std::string> groupTags = {"host=server01", "region=us-west"};

    AnomalyConfig config;
    config.algorithm = Algorithm::BASIC;
    config.bounds = 2.0;

    auto result = executor.execute(timestamps, values, groupTags, config);

    EXPECT_TRUE(result.success);
    EXPECT_GE(result.series.size(), 4u);

    // All pieces should have the same group tags
    for (const auto& piece : result.series) {
        EXPECT_EQ(piece.groupTags, groupTags)
            << "Group tags mismatch in piece: " << piece.piece;
    }
}

// ===========================================================================
// 12. Anomaly via executeFromJsonWithAnomaly
// ===========================================================================

TEST_F(AnomalyExecutorSeastarTest, ExecuteFromJsonWithAnomalyPath) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        uint64_t intervalNs = 60000000000ULL;
        uint64_t startNs = 1704067200000000000ULL;

        std::vector<std::pair<uint64_t, double>> points;
        for (size_t i = 0; i < 100; ++i) {
            double val = 50.0;
            if (i == 90) val = 400.0;
            points.push_back({startNs + i * intervalNs, val});
        }
        insertSeries(eng.eng, "disk", "iops", {{"vol", "ssd"}}, points);

        DerivedQueryExecutor executor(&eng.eng);

        std::string json = R"json({
            "queries": {
                "q": "avg:disk(iops){vol:ssd}"
            },
            "formula": "anomalies(q, 'basic', 2)",
            "startTime": 1704067200000000000,
            "endTime": 1704073200000000000
        })json";

        auto variantResult = executor.executeFromJsonWithAnomaly(json).get();

        ASSERT_TRUE(std::holds_alternative<AnomalyQueryResult>(variantResult));
        auto& result = std::get<AnomalyQueryResult>(variantResult);
        EXPECT_TRUE(result.success);
        EXPECT_GE(result.series.size(), 4u);
        EXPECT_EQ(result.statistics.algorithm, "basic");
    }).join().get();
}
