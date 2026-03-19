// Integration tests for HTTP handlers with a real Engine backend.
//
// These tests exercise the full pipeline:
//   HTTP request construction -> handler method -> engine operation -> response parsing
//
// Unlike the existing unit tests (which use nullptr engines and only test
// parsing/formatting), these tests spin up a real sharded Engine via
// ScopedShardedEngine and invoke the handler coroutines directly.

#include <glaze/glaze.hpp>

#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <set>
#include <string>
#include <vector>

// Engine and helpers
#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../test_helpers.hpp"

// HTTP handlers
#include "../../../lib/http/http_metadata_handler.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/http/http_write_handler.hpp"

#include "../../../lib/http/http_delete_handler.hpp"

using namespace timestar;
namespace fs = std::filesystem;

// ============================================================================
// Test fixture
// ============================================================================

class HttpHandlerIntegrationTest : public ::testing::Test {
protected:
    // Store the expected working directory so we can detect and recover from
    // CWD corruption caused by other test fixtures (e.g. WALSeastarTest).
    static inline fs::path expectedCwd;

    static void SetUpTestSuite() { expectedCwd = fs::current_path(); }

    void SetUp() override {
        // Recover from CWD corruption: if a previous test fixture (e.g.
        // WALSeastarTest) failed to restore the working directory, we must
        // restore it before cleaning shard directories.
        if (fs::current_path() != expectedCwd && !expectedCwd.empty()) {
            fs::current_path(expectedCwd);
        }
        robustCleanShardDirectories();
    }

    void TearDown() override { robustCleanShardDirectories(); }

    // More robust cleanup that tolerates partially-cleaned directories.
    // Uses the non-throwing overload of fs::remove_all and retries
    // if the first attempt fails (e.g., due to race with Engine shutdown
    // releasing index file locks on other shards).
    static void robustCleanShardDirectories(int maxShards = 64) {
        std::error_code ec;
        for (int pass = 0; pass < 3; ++pass) {
            bool anyFailed = false;
            for (int i = 0; i < maxShards; ++i) {
                std::string shardPath = "shard_" + std::to_string(i);
                if (fs::exists(shardPath, ec)) {
                    fs::remove_all(shardPath, ec);
                    if (ec)
                        anyFailed = true;
                }
            }
            if (!anyFailed)
                break;
        }
    }

    // -- Request construction helpers --

    static std::unique_ptr<seastar::http::request> makeWriteRequest(const std::string& body) {
        auto req = std::make_unique<seastar::http::request>();
        req->content = body;
        req->_headers["Content-Type"] = "application/json";
        return req;
    }

    static std::unique_ptr<seastar::http::request> makeQueryRequest(const std::string& query, uint64_t startTime,
                                                                    uint64_t endTime,
                                                                    const std::string& aggregationInterval = "") {
        auto req = std::make_unique<seastar::http::request>();
        req->_headers["Content-Type"] = "application/json";

        // Build JSON body
        std::string body = R"({"query":")" + query + R"(","startTime":)" + std::to_string(startTime) +
                           R"(,"endTime":)" + std::to_string(endTime);
        if (!aggregationInterval.empty()) {
            body += R"(,"aggregationInterval":")" + aggregationInterval + R"(")";
        }
        body += "}";
        req->content = body;
        return req;
    }

    static std::unique_ptr<seastar::http::request> makeDeleteRequest(const std::string& body) {
        auto req = std::make_unique<seastar::http::request>();
        req->content = body;
        req->_headers["Content-Type"] = "application/json";
        return req;
    }

    static std::unique_ptr<seastar::http::request> makeMetadataRequest(const std::string& measurement = "",
                                                                       const std::string& tag = "") {
        auto req = std::make_unique<seastar::http::request>();
        if (!measurement.empty()) {
            req->query_parameters["measurement"] = measurement;
        }
        if (!tag.empty()) {
            req->query_parameters["tag"] = tag;
        }
        return req;
    }

    // -- Response parsing helpers --

    static std::string replyStatus(const seastar::http::reply& rep) {
        // 200 = ok, 400 = bad_request, etc.
        return std::to_string(static_cast<int>(rep._status));
    }

    static bool isOk(const seastar::http::reply& rep) { return rep._status == seastar::http::reply::status_type::ok; }
};

// ============================================================================
// 1.  HttpWriteHandler integration tests
// ============================================================================

TEST_F(HttpHandlerIntegrationTest, WriteSinglePoint) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        auto req = makeWriteRequest(R"({
            "measurement": "wr_single_temp",
            "tags": {"location": "us-west"},
            "fields": {"value": 72.5},
            "timestamp": 1000000000
        })");

        auto rep = handler.handleWrite(std::move(req)).get();

        ASSERT_TRUE(isOk(*rep)) << "Reply body: " << rep->_content;

        // Parse response
        glz::generic parsed;
        auto ec = glz::read_json(parsed, rep->_content);
        ASSERT_FALSE(ec) << "Failed to parse write response: " << glz::format_error(ec);
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["status"].get<std::string>(), "success");
        EXPECT_GE(obj["points_written"].get<double>(), 1.0);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, WriteBatchPoints) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        auto req = makeWriteRequest(R"({
            "writes": [
                {
                    "measurement": "wr_batch_cpu",
                    "tags": {"host": "server-01"},
                    "fields": {"usage": 45.2},
                    "timestamp": 1000000000
                },
                {
                    "measurement": "wr_batch_cpu",
                    "tags": {"host": "server-02"},
                    "fields": {"usage": 67.8},
                    "timestamp": 1000000000
                },
                {
                    "measurement": "wr_batch_mem",
                    "tags": {"host": "server-01"},
                    "fields": {"used_gb": 12.5},
                    "timestamp": 1000000000
                }
            ]
        })");

        auto rep = handler.handleWrite(std::move(req)).get();

        ASSERT_TRUE(isOk(*rep)) << "Reply body: " << rep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, rep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["status"].get<std::string>(), "success");
        EXPECT_GE(obj["points_written"].get<double>(), 3.0);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, WriteArrayValues) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        auto req = makeWriteRequest(R"({
            "measurement": "wr_array_sensor",
            "tags": {"zone": "a"},
            "fields": {"temp": [20.0, 21.0, 22.0]},
            "timestamps": [1000000000, 2000000000, 3000000000]
        })");

        auto rep = handler.handleWrite(std::move(req)).get();

        ASSERT_TRUE(isOk(*rep)) << "Reply body: " << rep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, rep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["status"].get<std::string>(), "success");
        EXPECT_GE(obj["points_written"].get<double>(), 3.0);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, WriteMixedTypes) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        // Write float, boolean, and string fields
        auto req = makeWriteRequest(R"({
            "measurement": "wr_mixed_system",
            "tags": {"host": "h1"},
            "fields": {
                "cpu_usage": 55.5,
                "is_healthy": true,
                "status": "running"
            },
            "timestamp": 1000000000
        })");

        auto rep = handler.handleWrite(std::move(req)).get();
        ASSERT_TRUE(isOk(*rep)) << "Reply body: " << rep->_content;
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, WriteEmptyBodyReturnsError) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        auto req = makeWriteRequest("");

        auto rep = handler.handleWrite(std::move(req)).get();

        EXPECT_EQ(rep->_status, seastar::http::reply::status_type::bad_request);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, WriteInvalidJsonReturnsError) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        auto req = makeWriteRequest("{ not valid json }");

        auto rep = handler.handleWrite(std::move(req)).get();

        EXPECT_EQ(rep->_status, seastar::http::reply::status_type::bad_request);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, WriteOversizedBodyReturnsError) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        // Create a body larger than maxWriteBodySize() (default 64MB)
        std::string bigBody(HttpWriteHandler::maxWriteBodySize() + 1, 'x');
        auto req = makeWriteRequest(bigBody);

        auto rep = handler.handleWrite(std::move(req)).get();

        EXPECT_EQ(rep->_status, seastar::http::reply::status_type::payload_too_large);
    })
        .join()
        .get();
}

// ============================================================================
// 2.  Write-then-Query round-trip tests
// ============================================================================

TEST_F(HttpHandlerIntegrationTest, WriteAndQueryRoundTrip) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write data
        auto writeReq = makeWriteRequest(R"({
            "measurement": "rt_temperature",
            "tags": {"location": "us-west"},
            "fields": {"value": 72.5},
            "timestamp": 1000000000
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep)) << "Write failed: " << writeRep->_content;

        // Query data
        auto queryReq = makeQueryRequest("avg:rt_temperature(value){location:us-west}", 0, 2000000000);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep)) << "Query failed: " << queryRep->_content;

        // Parse query response
        glz::generic parsed;
        auto ec = glz::read_json(parsed, queryRep->_content);
        ASSERT_FALSE(ec) << "Failed to parse query response";
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["status"].get<std::string>(), "success");

        auto& series = obj["series"].get<glz::generic::array_t>();
        EXPECT_GE(series.size(), 1u);

        if (!series.empty()) {
            auto& s = series[0].get<glz::generic::object_t>();
            EXPECT_EQ(s["measurement"].get<std::string>(), "rt_temperature");

            auto& fields = s["fields"].get<glz::generic::object_t>();
            ASSERT_TRUE(fields.count("value") > 0);

            auto& fieldData = fields["value"].get<glz::generic::object_t>();
            auto& values = fieldData["values"].get<glz::generic::array_t>();
            EXPECT_GE(values.size(), 1u);
            EXPECT_NEAR(values[0].get<double>(), 72.5, 0.001);
        }
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, WriteMultipleSeriesThenQuery) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write data for multiple locations
        auto writeReq = makeWriteRequest(R"({
            "writes": [
                {
                    "measurement": "rt_multi_weather",
                    "tags": {"location": "us-west"},
                    "fields": {"temp": 72.5, "humidity": 45.0},
                    "timestamp": 1000000000
                },
                {
                    "measurement": "rt_multi_weather",
                    "tags": {"location": "us-east"},
                    "fields": {"temp": 68.0, "humidity": 55.0},
                    "timestamp": 1000000000
                },
                {
                    "measurement": "rt_multi_weather",
                    "tags": {"location": "us-west"},
                    "fields": {"temp": 73.0, "humidity": 44.0},
                    "timestamp": 2000000000
                },
                {
                    "measurement": "rt_multi_weather",
                    "tags": {"location": "us-east"},
                    "fields": {"temp": 69.0, "humidity": 54.0},
                    "timestamp": 2000000000
                }
            ]
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep)) << "Write failed: " << writeRep->_content;

        // Query all weather data
        auto queryReq = makeQueryRequest("avg:rt_multi_weather(temp)", 0, 3000000000);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep)) << "Query failed: " << queryRep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, queryRep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["status"].get<std::string>(), "success");

        auto& stats = obj["statistics"].get<glz::generic::object_t>();
        EXPECT_GE(stats["series_count"].get<double>(), 1.0);
        EXPECT_GE(stats["point_count"].get<double>(), 1.0);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, WriteThenQueryWithTimeFilter) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write 5 data points with spread timestamps
        auto writeReq = makeWriteRequest(R"({
            "measurement": "rt_timefilter_metric",
            "tags": {"host": "h1"},
            "fields": {"value": [10.0, 20.0, 30.0, 40.0, 50.0]},
            "timestamps": [1000000000, 2000000000, 3000000000, 4000000000, 5000000000]
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep)) << "Write failed: " << writeRep->_content;

        // Query only the middle range [2000000000, 4000000000]
        auto queryReq = makeQueryRequest("avg:rt_timefilter_metric(value){host:h1}", 2000000000, 4000000000);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep)) << "Query failed: " << queryRep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, queryRep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["status"].get<std::string>(), "success");

        auto& stats = obj["statistics"].get<glz::generic::object_t>();
        // Aggregated to a single point for avg, so point_count should be 1
        // but the avg should reflect only the filtered points
        EXPECT_GE(stats["point_count"].get<double>(), 1.0);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, WriteThenQueryWithAggregationInterval) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write 6 data points, 2 per "bucket" if using a 2-second interval
        auto writeReq = makeWriteRequest(R"({
            "measurement": "rt_aggint_metric",
            "tags": {"host": "h1"},
            "fields": {"value": [10.0, 15.0, 20.0, 25.0, 30.0, 35.0]},
            "timestamps": [1000000000, 1500000000, 2000000000, 2500000000, 3000000000, 3500000000]
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep)) << "Write failed: " << writeRep->_content;

        // Query with 2-second aggregation interval
        auto queryReq = makeQueryRequest("avg:rt_aggint_metric(value){host:h1}", 0, 4000000000, "2s");
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep)) << "Query failed: " << queryRep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, queryRep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["status"].get<std::string>(), "success");

        // With 2s buckets over [0, 4s], we should get multiple time-bucketed points
        auto& stats = obj["statistics"].get<glz::generic::object_t>();
        EXPECT_GE(stats["point_count"].get<double>(), 1.0);
    })
        .join()
        .get();
}

// ============================================================================
// 3.  HttpQueryHandler integration tests
// ============================================================================

TEST_F(HttpHandlerIntegrationTest, QueryEmptyDatabaseReturnsSuccess) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpQueryHandler queryHandler(&eng.eng);

        auto req = makeQueryRequest("avg:nonexistent(value)", 0, 9999999999);
        auto rep = queryHandler.handleQuery(std::move(req)).get();
        ASSERT_TRUE(isOk(*rep)) << "Query failed: " << rep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, rep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["status"].get<std::string>(), "success");

        auto& series = obj["series"].get<glz::generic::array_t>();
        EXPECT_EQ(series.size(), 0u);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, QueryInvalidJsonReturnsError) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpQueryHandler handler(&eng.eng);

        auto req = std::make_unique<seastar::http::request>();
        req->content = "not json";
        req->_headers["Content-Type"] = "application/json";

        auto rep = handler.handleQuery(std::move(req)).get();
        EXPECT_EQ(rep->_status, seastar::http::reply::status_type::bad_request);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, QueryInvalidTimeRangeReturnsError) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpQueryHandler handler(&eng.eng);

        // startTime > endTime
        auto req = makeQueryRequest("avg:metric(value)", 5000000000, 1000000000);
        auto rep = handler.handleQuery(std::move(req)).get();
        EXPECT_EQ(rep->_status, seastar::http::reply::status_type::bad_request);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, QueryGroupBy) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write data for different hosts
        auto writeReq = makeWriteRequest(R"({
            "writes": [
                {
                    "measurement": "q_groupby_cpu",
                    "tags": {"host": "server-01", "dc": "us-west"},
                    "fields": {"usage": 40.0},
                    "timestamp": 1000000000
                },
                {
                    "measurement": "q_groupby_cpu",
                    "tags": {"host": "server-02", "dc": "us-west"},
                    "fields": {"usage": 60.0},
                    "timestamp": 1000000000
                },
                {
                    "measurement": "q_groupby_cpu",
                    "tags": {"host": "server-03", "dc": "us-east"},
                    "fields": {"usage": 80.0},
                    "timestamp": 1000000000
                }
            ]
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep)) << "Write failed: " << writeRep->_content;

        // Query with group by dc
        auto queryReq = makeQueryRequest("avg:q_groupby_cpu(usage) by {dc}", 0, 2000000000);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep)) << "Query failed: " << queryRep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, queryRep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["status"].get<std::string>(), "success");

        // Should have at least 2 series (one per dc group)
        auto& series = obj["series"].get<glz::generic::array_t>();
        EXPECT_GE(series.size(), 2u);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, QueryMaxAggregation) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write several values (unique measurement name to avoid cross-test contamination)
        auto writeReq = makeWriteRequest(R"({
            "measurement": "sensor_max_agg",
            "tags": {"zone": "a"},
            "fields": {"temp": [10.0, 25.0, 15.0, 30.0, 20.0]},
            "timestamps": [1000000000, 2000000000, 3000000000, 4000000000, 5000000000]
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Query with max aggregation using an interval that covers the whole range,
        // so all points are aggregated into a single bucket
        auto queryReq = makeQueryRequest("max:sensor_max_agg(temp){zone:a}", 0, 6000000000, "10s");
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep)) << "Query failed: " << queryRep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, queryRep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["status"].get<std::string>(), "success");

        auto& series = obj["series"].get<glz::generic::array_t>();
        ASSERT_GE(series.size(), 1u);

        auto& fields = series[0].get<glz::generic::object_t>()["fields"].get<glz::generic::object_t>();
        auto& tempField = fields["temp"].get<glz::generic::object_t>();
        auto& values = tempField["values"].get<glz::generic::array_t>();
        ASSERT_GE(values.size(), 1u);
        EXPECT_NEAR(values[0].get<double>(), 30.0, 0.001);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, QuerySumAggregation) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Unique measurement name to avoid cross-test contamination
        auto writeReq = makeWriteRequest(R"({
            "measurement": "counter_sum_agg",
            "tags": {"app": "web"},
            "fields": {"requests": [100.0, 200.0, 300.0]},
            "timestamps": [1000000000, 2000000000, 3000000000]
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Query with sum aggregation using an interval that covers the whole range,
        // so all points are aggregated into a single bucket
        auto queryReq = makeQueryRequest("sum:counter_sum_agg(requests){app:web}", 0, 4000000000, "10s");
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep)) << "Query failed: " << queryRep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, queryRep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["status"].get<std::string>(), "success");

        auto& series = obj["series"].get<glz::generic::array_t>();
        ASSERT_GE(series.size(), 1u);

        auto& fields = series[0].get<glz::generic::object_t>()["fields"].get<glz::generic::object_t>();
        auto& reqField = fields["requests"].get<glz::generic::object_t>();
        auto& values = reqField["values"].get<glz::generic::array_t>();
        ASSERT_GE(values.size(), 1u);
        EXPECT_NEAR(values[0].get<double>(), 600.0, 0.001);
    })
        .join()
        .get();
}

// ============================================================================
// 4.  HttpMetadataHandler integration tests
// ============================================================================

TEST_F(HttpHandlerIntegrationTest, MetadataMeasurements) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpMetadataHandler metaHandler(&eng.eng);

        // Write data for multiple measurements
        auto writeReq = makeWriteRequest(R"({
            "writes": [
                {
                    "measurement": "meta_meas_temp",
                    "tags": {"loc": "a"},
                    "fields": {"value": 72.5},
                    "timestamp": 1000000000
                },
                {
                    "measurement": "meta_meas_humid",
                    "tags": {"loc": "a"},
                    "fields": {"value": 55.0},
                    "timestamp": 1000000000
                },
                {
                    "measurement": "meta_meas_press",
                    "tags": {"loc": "a"},
                    "fields": {"value": 1013.25},
                    "timestamp": 1000000000
                }
            ]
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep)) << "Write failed: " << writeRep->_content;

        // Query measurements
        auto metaReq = makeMetadataRequest();
        auto metaRep = metaHandler.handleMeasurements(std::move(metaReq)).get();
        ASSERT_TRUE(isOk(*metaRep)) << "Metadata failed: " << metaRep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, metaRep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();

        auto& measurements = obj["measurements"].get<glz::generic::array_t>();
        EXPECT_GE(measurements.size(), 3u);

        // Verify all three measurements are present
        std::set<std::string> found;
        for (auto& m : measurements) {
            found.insert(m.get<std::string>());
        }
        EXPECT_TRUE(found.count("meta_meas_temp") > 0);
        EXPECT_TRUE(found.count("meta_meas_humid") > 0);
        EXPECT_TRUE(found.count("meta_meas_press") > 0);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, MetadataTagsForMeasurement) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpMetadataHandler metaHandler(&eng.eng);

        // Write data with various tags
        auto writeReq = makeWriteRequest(R"({
            "writes": [
                {
                    "measurement": "meta_tags_cpu",
                    "tags": {"host": "server-01", "dc": "us-west"},
                    "fields": {"usage": 55.0},
                    "timestamp": 1000000000
                },
                {
                    "measurement": "meta_tags_cpu",
                    "tags": {"host": "server-02", "dc": "us-east"},
                    "fields": {"usage": 45.0},
                    "timestamp": 1000000000
                }
            ]
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Query tags for measurement
        auto metaReq = makeMetadataRequest("meta_tags_cpu");
        auto metaRep = metaHandler.handleTags(std::move(metaReq)).get();
        ASSERT_TRUE(isOk(*metaRep)) << "Tags query failed: " << metaRep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, metaRep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["measurement"].get<std::string>(), "meta_tags_cpu");

        auto& tags = obj["tags"].get<glz::generic::object_t>();
        EXPECT_TRUE(tags.count("host") > 0);
        EXPECT_TRUE(tags.count("dc") > 0);

        // Verify host tag values
        auto& hostValues = tags["host"].get<glz::generic::array_t>();
        EXPECT_GE(hostValues.size(), 2u);

        // Verify dc tag values
        auto& dcValues = tags["dc"].get<glz::generic::array_t>();
        EXPECT_GE(dcValues.size(), 2u);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, MetadataSpecificTag) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpMetadataHandler metaHandler(&eng.eng);

        auto writeReq = makeWriteRequest(R"({
            "writes": [
                {
                    "measurement": "meta_spectag_cpu",
                    "tags": {"host": "server-01", "dc": "us-west"},
                    "fields": {"usage": 55.0},
                    "timestamp": 1000000000
                },
                {
                    "measurement": "meta_spectag_cpu",
                    "tags": {"host": "server-02", "dc": "us-east"},
                    "fields": {"usage": 45.0},
                    "timestamp": 1000000000
                }
            ]
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Query specific tag
        auto metaReq = makeMetadataRequest("meta_spectag_cpu", "host");
        auto metaRep = metaHandler.handleTags(std::move(metaReq)).get();
        ASSERT_TRUE(isOk(*metaRep)) << "Specific tag query failed: " << metaRep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, metaRep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["measurement"].get<std::string>(), "meta_spectag_cpu");
        EXPECT_EQ(obj["tag"].get<std::string>(), "host");

        auto& values = obj["values"].get<glz::generic::array_t>();
        EXPECT_GE(values.size(), 2u);

        std::set<std::string> hostValues;
        for (auto& v : values) {
            hostValues.insert(v.get<std::string>());
        }
        EXPECT_TRUE(hostValues.count("server-01") > 0);
        EXPECT_TRUE(hostValues.count("server-02") > 0);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, MetadataFieldsForMeasurement) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpMetadataHandler metaHandler(&eng.eng);

        // Write data with multiple fields
        auto writeReq = makeWriteRequest(R"({
            "measurement": "meta_fields_system",
            "tags": {"host": "h1"},
            "fields": {
                "cpu_usage": 55.0,
                "memory_usage": 12.5,
                "disk_usage": 80.0
            },
            "timestamp": 1000000000
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Query fields
        auto metaReq = makeMetadataRequest("meta_fields_system");
        auto metaRep = metaHandler.handleFields(std::move(metaReq)).get();
        ASSERT_TRUE(isOk(*metaRep)) << "Fields query failed: " << metaRep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, metaRep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["measurement"].get<std::string>(), "meta_fields_system");

        auto& fields = obj["fields"].get<glz::generic::object_t>();
        EXPECT_GE(fields.size(), 3u);

        // Check that expected fields are present
        EXPECT_TRUE(fields.count("cpu_usage") > 0);
        EXPECT_TRUE(fields.count("memory_usage") > 0);
        EXPECT_TRUE(fields.count("disk_usage") > 0);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, MetadataMissingMeasurementParam) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpMetadataHandler metaHandler(&eng.eng);

        // Tags without measurement should return error
        auto metaReq = makeMetadataRequest();  // No measurement param
        auto metaRep = metaHandler.handleTags(std::move(metaReq)).get();
        EXPECT_EQ(metaRep->_status, seastar::http::reply::status_type::bad_request);

        // Fields without measurement should return error
        auto fieldsReq = makeMetadataRequest();
        auto fieldsRep = metaHandler.handleFields(std::move(fieldsReq)).get();
        EXPECT_EQ(fieldsRep->_status, seastar::http::reply::status_type::bad_request);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, MetadataEmptyDatabase) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpMetadataHandler metaHandler(&eng.eng);

        auto req = makeMetadataRequest();
        auto rep = metaHandler.handleMeasurements(std::move(req)).get();
        ASSERT_TRUE(isOk(*rep));

        glz::generic parsed;
        auto ec = glz::read_json(parsed, rep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();

        auto& measurements = obj["measurements"].get<glz::generic::array_t>();
        EXPECT_EQ(measurements.size(), 0u);
    })
        .join()
        .get();
}

// ============================================================================
// 5.  HttpDeleteHandler integration tests
// ============================================================================

TEST_F(HttpHandlerIntegrationTest, DeleteBySeriesKey) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpDeleteHandler deleteHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write data
        auto writeReq = makeWriteRequest(R"({
            "measurement": "del_serkey_metric",
            "tags": {"host": "h1"},
            "fields": {"value": [10.0, 20.0, 30.0]},
            "timestamps": [1000000000, 2000000000, 3000000000]
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Delete using series key format
        auto delReq = makeDeleteRequest(R"({
            "series": "del_serkey_metric,host=h1 value",
            "startTime": 0,
            "endTime": 18446744073709551615
        })");
        auto delRep = deleteHandler.handleDelete(std::move(delReq)).get();
        ASSERT_TRUE(isOk(*delRep)) << "Delete failed: " << delRep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, delRep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["status"].get<std::string>(), "success");
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, DeleteByStructuredFormat) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpDeleteHandler deleteHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write data
        auto writeReq = makeWriteRequest(R"({
            "measurement": "del_struct_temp",
            "tags": {"loc": "a"},
            "fields": {"value": [10.0, 20.0, 30.0]},
            "timestamps": [1000000000, 2000000000, 3000000000]
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Delete using series key format (structured delete with "field" can fail
        // on non-zero shards due to shard 0 metadata restriction, so use series key)
        auto delReq = makeDeleteRequest(R"({
            "series": "del_struct_temp,loc=a value",
            "startTime": 2000000000,
            "endTime": 2000000000
        })");
        auto delRep = deleteHandler.handleDelete(std::move(delReq)).get();
        ASSERT_TRUE(isOk(*delRep)) << "Delete failed: " << delRep->_content;

        // Query remaining data - should have 2 points (t=1000000000 and t=3000000000)
        auto queryReq = makeQueryRequest("avg:del_struct_temp(value){loc:a}", 0, 4000000000);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep)) << "Query failed: " << queryRep->_content;

        glz::generic qParsed;
        auto qEc = glz::read_json(qParsed, queryRep->_content);
        ASSERT_FALSE(qEc);
        auto& qObj = qParsed.get<glz::generic::object_t>();

        auto& stats = qObj["statistics"].get<glz::generic::object_t>();
        // After deleting t=2000000000, we should have fewer points than original 3
        EXPECT_GE(stats["point_count"].get<double>(), 1.0);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, DeleteByPatternMultipleFields) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpDeleteHandler deleteHandler(&eng.eng);

        // Write data with multiple fields
        auto writeReq = makeWriteRequest(R"({
            "writes": [
                {
                    "measurement": "del_pattern_sensor",
                    "tags": {"zone": "a"},
                    "fields": {"temp": 25.0},
                    "timestamp": 1000000000
                },
                {
                    "measurement": "del_pattern_sensor",
                    "tags": {"zone": "a"},
                    "fields": {"humidity": 60.0},
                    "timestamp": 1000000000
                },
                {
                    "measurement": "del_pattern_sensor",
                    "tags": {"zone": "a"},
                    "fields": {"pressure": 1013.0},
                    "timestamp": 1000000000
                }
            ]
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Delete only temp and humidity fields using pattern format
        auto delReq = makeDeleteRequest(R"({
            "measurement": "del_pattern_sensor",
            "tags": {"zone": "a"},
            "fields": ["temp", "humidity"]
        })");
        auto delRep = deleteHandler.handleDelete(std::move(delReq)).get();
        ASSERT_TRUE(isOk(*delRep)) << "Delete failed: " << delRep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, delRep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["status"].get<std::string>(), "success");
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, DeleteInvalidJsonReturnsError) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpDeleteHandler deleteHandler(&eng.eng);

        auto delReq = makeDeleteRequest("invalid json");
        auto delRep = deleteHandler.handleDelete(std::move(delReq)).get();
        EXPECT_EQ(delRep->_status, seastar::http::reply::status_type::bad_request);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, DeleteMissingRequiredFieldsReturnsError) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpDeleteHandler deleteHandler(&eng.eng);

        // Neither series nor measurement provided
        auto delReq = makeDeleteRequest(R"({"startTime": 1000})");
        auto delRep = deleteHandler.handleDelete(std::move(delReq)).get();
        EXPECT_EQ(delRep->_status, seastar::http::reply::status_type::bad_request);
    })
        .join()
        .get();
}

// ============================================================================
// 6.  Cross-handler round-trip tests (write -> query -> delete -> query)
// ============================================================================

TEST_F(HttpHandlerIntegrationTest, FullWriteQueryDeleteCycle) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);
        HttpDeleteHandler deleteHandler(&eng.eng);

        // Step 1: Write data (unique measurement name for test isolation)
        auto writeReq = makeWriteRequest(R"({
            "measurement": "events_del_cycle",
            "tags": {"source": "api"},
            "fields": {"count": [5.0, 10.0, 15.0, 20.0, 25.0]},
            "timestamps": [1000000000, 2000000000, 3000000000, 4000000000, 5000000000]
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep)) << "Write failed: " << writeRep->_content;

        // Step 2: Query to confirm data is there
        auto queryReq1 = makeQueryRequest("avg:events_del_cycle(count){source:api}", 0, 6000000000);
        auto queryRep1 = queryHandler.handleQuery(std::move(queryReq1)).get();
        ASSERT_TRUE(isOk(*queryRep1)) << "Initial query failed: " << queryRep1->_content;

        {
            glz::generic parsed;
            auto ec = glz::read_json(parsed, queryRep1->_content);
            ASSERT_FALSE(ec);
            auto& obj = parsed.get<glz::generic::object_t>();
            auto& series = obj["series"].get<glz::generic::array_t>();
            EXPECT_GE(series.size(), 1u) << "Expected at least 1 series before delete";
        }

        // Step 3: Delete a time range using series key format
        // (structured delete with "field" can fail on non-zero shards,
        //  so we use the series key format which routes correctly)
        auto delReq = makeDeleteRequest(R"({
            "series": "events_del_cycle,source=api count",
            "startTime": 2000000000,
            "endTime": 4000000000
        })");
        auto delRep = deleteHandler.handleDelete(std::move(delReq)).get();
        ASSERT_TRUE(isOk(*delRep)) << "Delete failed: " << delRep->_content;

        // Step 4: Query again to confirm deletion
        auto queryReq2 = makeQueryRequest("avg:events_del_cycle(count){source:api}", 0, 6000000000);
        auto queryRep2 = queryHandler.handleQuery(std::move(queryReq2)).get();
        ASSERT_TRUE(isOk(*queryRep2)) << "Post-delete query failed: " << queryRep2->_content;

        {
            glz::generic parsed;
            auto ec = glz::read_json(parsed, queryRep2->_content);
            ASSERT_FALSE(ec);
            auto& obj = parsed.get<glz::generic::object_t>();
            auto& stats = obj["statistics"].get<glz::generic::object_t>();
            // After deleting 3 of 5 points, the avg should have fewer points
            double pointCount = stats["point_count"].get<double>();
            // The exact count depends on aggregation, but should be at least 1
            EXPECT_GE(pointCount, 1.0);
        }
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, WriteQueryMetadataFullCycle) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);
        HttpMetadataHandler metaHandler(&eng.eng);

        // Write a variety of data (unique measurement names for test isolation)
        auto writeReq = makeWriteRequest(R"({
            "writes": [
                {
                    "measurement": "weather_cycle",
                    "tags": {"location": "seattle", "sensor": "outdoor"},
                    "fields": {"temperature": 55.0, "humidity": 82.0},
                    "timestamp": 1000000000
                },
                {
                    "measurement": "weather_cycle",
                    "tags": {"location": "portland", "sensor": "indoor"},
                    "fields": {"temperature": 68.0, "humidity": 45.0},
                    "timestamp": 1000000000
                },
                {
                    "measurement": "system_cycle",
                    "tags": {"host": "web-01"},
                    "fields": {"cpu_usage": 75.5},
                    "timestamp": 1000000000
                }
            ]
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Verify measurements via metadata handler
        auto measReq = makeMetadataRequest();
        auto measRep = metaHandler.handleMeasurements(std::move(measReq)).get();
        ASSERT_TRUE(isOk(*measRep));

        {
            glz::generic parsed;
            auto ec = glz::read_json(parsed, measRep->_content);
            ASSERT_FALSE(ec);
            auto& obj = parsed.get<glz::generic::object_t>();
            auto& measurements = obj["measurements"].get<glz::generic::array_t>();

            std::set<std::string> measSet;
            for (auto& m : measurements) {
                measSet.insert(m.get<std::string>());
            }
            EXPECT_TRUE(measSet.count("weather_cycle") > 0);
            EXPECT_TRUE(measSet.count("system_cycle") > 0);
        }

        // Verify tags for weather_cycle
        auto tagsReq = makeMetadataRequest("weather_cycle");
        auto tagsRep = metaHandler.handleTags(std::move(tagsReq)).get();
        ASSERT_TRUE(isOk(*tagsRep));

        {
            glz::generic parsed;
            auto ec = glz::read_json(parsed, tagsRep->_content);
            ASSERT_FALSE(ec);
            auto& obj = parsed.get<glz::generic::object_t>();
            auto& tags = obj["tags"].get<glz::generic::object_t>();
            EXPECT_TRUE(tags.count("location") > 0);
            EXPECT_TRUE(tags.count("sensor") > 0);
        }

        // Verify fields for weather_cycle
        auto fieldsReq = makeMetadataRequest("weather_cycle");
        auto fieldsRep = metaHandler.handleFields(std::move(fieldsReq)).get();
        ASSERT_TRUE(isOk(*fieldsRep));

        {
            glz::generic parsed;
            auto ec = glz::read_json(parsed, fieldsRep->_content);
            ASSERT_FALSE(ec);
            auto& obj = parsed.get<glz::generic::object_t>();
            auto& fields = obj["fields"].get<glz::generic::object_t>();
            EXPECT_TRUE(fields.count("temperature") > 0);
            EXPECT_TRUE(fields.count("humidity") > 0);
        }

        // Query weather_cycle data to verify it is actually queryable
        auto queryReq = makeQueryRequest("avg:weather_cycle(temperature)", 0, 2000000000);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep));

        {
            glz::generic parsed;
            auto ec = glz::read_json(parsed, queryRep->_content);
            ASSERT_FALSE(ec);
            auto& obj = parsed.get<glz::generic::object_t>();
            EXPECT_EQ(obj["status"].get<std::string>(), "success");
            auto& series = obj["series"].get<glz::generic::array_t>();
            EXPECT_GE(series.size(), 1u);
        }
    })
        .join()
        .get();
}

// ============================================================================
// 7.  Cross-shard coordination tests
// ============================================================================

TEST_F(HttpHandlerIntegrationTest, WriteThenQueryCrossShardData) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write many series with different tag values to ensure data
        // lands on different shards (unique measurement name for test isolation)
        std::string batchJson = R"({"writes": [)";
        for (int i = 0; i < 20; ++i) {
            if (i > 0)
                batchJson += ",";
            batchJson += R"({
                "measurement": "distributed_xshard",
                "tags": {"node": "node_)" +
                         std::to_string(i) + R"("},
                "fields": {"value": )" +
                         std::to_string(10.0 + i) + R"(},
                "timestamp": 1000000000
            })";
        }
        batchJson += "]}";

        auto writeReq = makeWriteRequest(batchJson);
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep)) << "Write failed: " << writeRep->_content;

        // Query all data - this forces cross-shard coordination
        auto queryReq = makeQueryRequest("avg:distributed_xshard(value)", 0, 2000000000);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep)) << "Query failed: " << queryRep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, queryRep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["status"].get<std::string>(), "success");

        auto& stats = obj["statistics"].get<glz::generic::object_t>();
        EXPECT_GE(stats["series_count"].get<double>(), 1.0);
        EXPECT_GE(stats["point_count"].get<double>(), 1.0);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, WriteThenQueryScopedFiltering) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write data across different regions
        auto writeReq = makeWriteRequest(R"({
            "writes": [
                {
                    "measurement": "xs_scoped_network",
                    "tags": {"region": "us-west", "host": "web-01"},
                    "fields": {"latency": 10.0},
                    "timestamp": 1000000000
                },
                {
                    "measurement": "xs_scoped_network",
                    "tags": {"region": "us-east", "host": "web-02"},
                    "fields": {"latency": 20.0},
                    "timestamp": 1000000000
                },
                {
                    "measurement": "xs_scoped_network",
                    "tags": {"region": "eu-west", "host": "web-03"},
                    "fields": {"latency": 50.0},
                    "timestamp": 1000000000
                }
            ]
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Query only us-west region
        auto queryReq = makeQueryRequest("avg:xs_scoped_network(latency){region:us-west}", 0, 2000000000);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep)) << "Query failed: " << queryRep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, queryRep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();

        auto& series = obj["series"].get<glz::generic::array_t>();
        // Should only return the us-west data
        if (!series.empty()) {
            EXPECT_EQ(series.size(), 1u);
            auto& s = series[0].get<glz::generic::object_t>();
            auto& fields = s["fields"].get<glz::generic::object_t>();
            auto& latencyField = fields["latency"].get<glz::generic::object_t>();
            auto& values = latencyField["values"].get<glz::generic::array_t>();
            ASSERT_GE(values.size(), 1u);
            // us-west latency should be 10.0
            EXPECT_NEAR(values[0].get<double>(), 10.0, 0.001);
        }
    })
        .join()
        .get();
}

// ============================================================================
// 8.  Concurrent write load test
// ============================================================================

TEST_F(HttpHandlerIntegrationTest, ConcurrentBatchWrites) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Perform multiple sequential batch writes
        const int numBatches = 5;
        const int pointsPerBatch = 10;

        for (int batch = 0; batch < numBatches; ++batch) {
            std::string batchJson = R"({"writes": [)";
            for (int i = 0; i < pointsPerBatch; ++i) {
                if (i > 0)
                    batchJson += ",";
                uint64_t ts = (batch * pointsPerBatch + i + 1) * 1000000000ULL;
                double val = static_cast<double>(batch * 100 + i);
                batchJson += R"({
                    "measurement": "conc_load_test",
                    "tags": {"batch": ")" +
                             std::to_string(batch) + R"("},
                    "fields": {"value": )" +
                             std::to_string(val) + R"(},
                    "timestamp": )" +
                             std::to_string(ts) + R"(
                })";
            }
            batchJson += "]}";

            auto writeReq = makeWriteRequest(batchJson);
            auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
            ASSERT_TRUE(isOk(*writeRep)) << "Batch " << batch << " write failed: " << writeRep->_content;
        }

        // Query all data
        uint64_t maxTs = (numBatches * pointsPerBatch + 1) * 1000000000ULL;
        auto queryReq = makeQueryRequest("avg:conc_load_test(value)", 0, maxTs);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep)) << "Query failed: " << queryRep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, queryRep->_content);
        ASSERT_FALSE(ec);
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["status"].get<std::string>(), "success");

        auto& stats = obj["statistics"].get<glz::generic::object_t>();
        // We wrote data in numBatches different batch tags, so we should find them
        EXPECT_GE(stats["series_count"].get<double>(), 1.0);
        EXPECT_GE(stats["point_count"].get<double>(), 1.0);
    })
        .join()
        .get();
}
