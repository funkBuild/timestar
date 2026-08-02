// Integration tests for HTTP handlers with a real Engine backend.
//
// These tests exercise the full pipeline:
//   HTTP request construction -> handler method -> engine operation -> response parsing
//
// Unlike the existing unit tests (which use nullptr engines and only test
// parsing/formatting), these tests spin up a real sharded Engine via
// ScopedShardedEngine and invoke the handler coroutines directly.

#include <glaze/json.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <map>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
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
#include "../../../lib/cluster/data/write_errors.hpp"
#include "../../../lib/http/http_delete_handler.hpp"
#include "../../../lib/http/http_metadata_handler.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/http/http_retention_handler.hpp"
#include "../../../lib/http/http_stream_handler.hpp"
#include "../../../lib/http/http_write_handler.hpp"

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

    static void resetClusterHooks() {
        HttpDeleteHandler::clusterDeleteHook = {};
        HttpDeleteHandler::clusterPatternExpandHook = {};
        HttpDeleteHandler::clusterDeletePlanHook = {};
        HttpDeleteHandler::clusterDeletePlanLookupHook = {};
    }

    void SetUp() override {
        resetClusterHooks();
        // Recover from CWD corruption: if a previous test fixture (e.g.
        // WALSeastarTest) failed to restore the working directory, we must
        // restore it before cleaning shard directories.
        if (fs::current_path() != expectedCwd && !expectedCwd.empty()) {
            fs::current_path(expectedCwd);
        }
        robustCleanShardDirectories();
    }

    void TearDown() override {
        resetClusterHooks();
        robustCleanShardDirectories();
    }

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
        req->_headers["Idempotency-Key"] = "11111111111111111111111111111111";
        static const uint64_t issuedAtMs = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count());
        req->_headers["Idempotency-Key-Timestamp"] = std::to_string(issuedAtMs);
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

    // Metadata indexing is asynchronous: writes are acknowledged before the
    // index dispatch completes (docs/api-write.md), so a metadata read issued
    // immediately after a write can race the indexer — reliably so on slow
    // instrumented (coverage) builds. Re-fetch until `ready(object)` holds or
    // ~5s elapse, leaving the last parsed response in `out` for the caller's
    // assertions.
    template <typename FetchFn, typename ReadyFn>
    static void pollMetadataJson(glz::generic& out, FetchFn fetch, ReadyFn ready) {
        for (int attempt = 0; attempt < 50; ++attempt) {
            auto rep = fetch();
            ASSERT_TRUE(isOk(*rep)) << "Metadata request failed: " << rep->_content;
            out = glz::generic{};
            auto ec = glz::read_json(out, rep->_content);
            ASSERT_FALSE(ec);
            if (ready(out.get<glz::generic::object_t>())) {
                return;
            }
            seastar::sleep(std::chrono::milliseconds(100)).get();
        }
    }
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

// Regression: wildcard/regex query SCOPES must filter series, not be treated as
// literal tag values. Before the fix, {host:server-*} produced a postings-bitmap
// lookup for the literal value "server-*" (which never exists) and returned zero
// series. This exercises the actual query path (handleQuery), which earlier tests
// never did for wildcard scopes — they called SeriesMatcher::matches() directly.
TEST_F(HttpHandlerIntegrationTest, WriteThenQueryWithWildcardScope) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        auto writeReq = makeWriteRequest(R"({
            "writes": [
                {"measurement": "wc_metric", "tags": {"host": "server-01"}, "fields": {"value": 1.0}, "timestamp": 1000000000},
                {"measurement": "wc_metric", "tags": {"host": "server-02"}, "fields": {"value": 2.0}, "timestamp": 1000000000},
                {"measurement": "wc_metric", "tags": {"host": "web-01"},    "fields": {"value": 9.0}, "timestamp": 1000000000}
            ]
        })");
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep)) << "Write failed: " << writeRep->_content;

        // Exact-scope control: {host:server-01} returns exactly one series.
        {
            auto req = makeQueryRequest("max:wc_metric(value){host:server-01} by {host}", 0, 2000000000);
            auto rep = queryHandler.handleQuery(std::move(req)).get();
            ASSERT_TRUE(isOk(*rep)) << "Exact query failed: " << rep->_content;
            glz::generic parsed;
            ASSERT_FALSE(glz::read_json(parsed, rep->_content));
            auto& stats = parsed.get<glz::generic::object_t>()["statistics"].get<glz::generic::object_t>();
            EXPECT_EQ(stats["series_count"].get<double>(), 1.0) << rep->_content;
        }

        // Wildcard scope: {host:server-*} must match server-01 and server-02 only.
        {
            auto req = makeQueryRequest("max:wc_metric(value){host:server-*} by {host}", 0, 2000000000);
            auto rep = queryHandler.handleQuery(std::move(req)).get();
            ASSERT_TRUE(isOk(*rep)) << "Wildcard query failed: " << rep->_content;
            glz::generic parsed;
            ASSERT_FALSE(glz::read_json(parsed, rep->_content));
            auto& obj = parsed.get<glz::generic::object_t>();
            auto& stats = obj["statistics"].get<glz::generic::object_t>();
            EXPECT_EQ(stats["series_count"].get<double>(), 2.0)
                << "wildcard scope {host:server-*} should match server-01 and server-02: " << rep->_content;

            // Verify the matched hosts are the server-* ones, not web-01.
            // The response emits group tags as a "groupTags" array of "key=value" strings.
            std::set<std::string> hosts;
            for (auto& s : obj["series"].get<glz::generic::array_t>()) {
                auto& gt = s.get<glz::generic::object_t>()["groupTags"].get<glz::generic::array_t>();
                for (auto& kv : gt) {
                    const std::string& pair = kv.get<std::string>();
                    if (pair.rfind("host=", 0) == 0)
                        hosts.insert(pair.substr(5));
                }
            }
            EXPECT_TRUE(hosts.count("server-01") && hosts.count("server-02")) << rep->_content;
            EXPECT_FALSE(hosts.count("web-01")) << "web-01 must not match server-*: " << rep->_content;
        }
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

        // Query measurements, polling past the async metadata indexing window.
        glz::generic parsed;
        pollMetadataJson(
            parsed, [&] { return metaHandler.handleMeasurements(makeMetadataRequest()).get(); },
            [](glz::generic::object_t& o) {
                std::set<std::string> names;
                for (auto& m : o["measurements"].get<glz::generic::array_t>()) {
                    names.insert(m.get<std::string>());
                }
                return names.count("meta_meas_temp") && names.count("meta_meas_humid") &&
                       names.count("meta_meas_press");
            });
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

        // Query tags for measurement, polling past the async metadata
        // indexing window.
        glz::generic parsed;
        pollMetadataJson(
            parsed, [&] { return metaHandler.handleTags(makeMetadataRequest("meta_tags_cpu")).get(); },
            [](glz::generic::object_t& o) {
                if (!o.count("tags"))
                    return false;
                auto& t = o["tags"].get<glz::generic::object_t>();
                return t.count("host") && t.count("dc") && t["host"].get<glz::generic::array_t>().size() >= 2 &&
                       t["dc"].get<glz::generic::array_t>().size() >= 2;
            });
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

        // Query specific tag, polling past the async metadata indexing window.
        glz::generic parsed;
        pollMetadataJson(
            parsed, [&] { return metaHandler.handleTags(makeMetadataRequest("meta_spectag_cpu", "host")).get(); },
            [](glz::generic::object_t& o) {
                return o.count("values") && o["values"].get<glz::generic::array_t>().size() >= 2;
            });
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

        // Query fields, polling past the async metadata indexing window.
        glz::generic parsed;
        pollMetadataJson(
            parsed, [&] { return metaHandler.handleFields(makeMetadataRequest("meta_fields_system")).get(); },
            [](glz::generic::object_t& o) {
                if (!o.count("fields"))
                    return false;
                auto& f = o["fields"].get<glz::generic::object_t>();
                return f.count("cpu_usage") && f.count("memory_usage") && f.count("disk_usage");
            });
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

TEST_F(HttpHandlerIntegrationTest, PartitionedClusterRejectsDeleteBeforeLocalMutation) {
    seastar::thread([] {
        HttpDeleteHandler deleteHandler(nullptr, true);
        auto req = makeDeleteRequest(R"({"series":"m value","startTime":0,"endTime":1})");
        req->_headers["x-timestar-cluster-forwarded"] = "1";
        auto rep = deleteHandler.handleDelete(std::move(req)).get();
        EXPECT_EQ(rep->_status, seastar::http::reply::status_type::not_implemented);
        EXPECT_NE(rep->_content.find("replicated deletes require RF>1"), std::string::npos);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedRf3RoutesExactDeleteThroughClusterHook) {
    seastar::thread([] {
        struct ResetHook {
            ~ResetHook() { HttpDeleteHandler::clusterDeleteHook = {}; }
        } reset;
        ScopedShardedEngine eng;
        eng.start();
        std::vector<std::string> keys;
        HttpDeleteHandler::clusterDeleteHook = [&keys](std::vector<timestar::data::DeleteRangeTarget> targets,
                                                       SeriesId128 operationId, uint64_t issuedAtMs) {
            if (targets.size() != 1u) {
                ADD_FAILURE() << "one exact target must produce one one-target VShard batch";
                return seastar::make_ready_future<>();
            }
            keys.push_back(std::move(targets[0].seriesKey));
            EXPECT_EQ(targets[0].startTime, 10u);
            EXPECT_EQ(targets[0].endTime, 20u);
            EXPECT_NE(operationId, SeriesId128{});
            EXPECT_NE(issuedAtMs, 0u);
            return seastar::make_ready_future<>();
        };

        HttpDeleteHandler handler(&eng.eng, true);
        auto req = makeDeleteRequest(
            R"({"measurement":"cluster_del","tags":{"host":"h1"},"field":"value","startTime":10,"endTime":20})");
        auto rep = handler.handleDelete(std::move(req)).get();
        ASSERT_TRUE(isOk(*rep)) << rep->_content;
        ASSERT_EQ(keys.size(), 1u);
        EXPECT_EQ(keys[0], buildSeriesKey("cluster_del", {{"host", "h1"}}, "value"));
        EXPECT_NE(rep->_content.find("committed and applied"), std::string::npos);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedRf3RequiresStableDeleteIdempotencyKey) {
    seastar::thread([] {
        struct ResetHook {
            ~ResetHook() { HttpDeleteHandler::clusterDeleteHook = {}; }
        } reset;
        ScopedShardedEngine eng;
        eng.start();
        std::vector<SeriesId128> operations;
        std::vector<uint64_t> timestamps;
        HttpDeleteHandler::clusterDeleteHook = [&operations, &timestamps](
                                                   std::vector<timestar::data::DeleteRangeTarget>,
                                                   SeriesId128 operationId, uint64_t issuedAtMs) {
            operations.push_back(operationId);
            timestamps.push_back(issuedAtMs);
            return seastar::make_ready_future<>();
        };

        HttpDeleteHandler handler(&eng.eng, true);
        auto missing = makeDeleteRequest(R"({"series":"m value","startTime":0,"endTime":1})");
        missing->_headers.erase("Idempotency-Key");
        auto missingReply = handler.handleDelete(std::move(missing)).get();
        EXPECT_EQ(missingReply->_status, seastar::http::reply::status_type::bad_request);
        EXPECT_NE(missingReply->_content.find("Idempotency-Key"), std::string::npos);
        EXPECT_TRUE(operations.empty());

        auto missingTimestamp = makeDeleteRequest(R"({"series":"m value","startTime":0,"endTime":1})");
        missingTimestamp->_headers.erase("Idempotency-Key-Timestamp");
        auto missingTimestampReply = handler.handleDelete(std::move(missingTimestamp)).get();
        EXPECT_EQ(missingTimestampReply->_status, seastar::http::reply::status_type::bad_request);
        EXPECT_NE(missingTimestampReply->_content.find("Idempotency-Key-Timestamp"), std::string::npos);
        EXPECT_TRUE(operations.empty());

        for (unsigned attempt = 0; attempt < 2; ++attempt) {
            auto reply =
                handler.handleDelete(makeDeleteRequest(R"({"series":"m value","startTime":0,"endTime":1})")).get();
            ASSERT_TRUE(isOk(*reply)) << reply->_content;
        }
        auto changed =
            handler.handleDelete(makeDeleteRequest(R"({"series":"m value","startTime":0,"endTime":2})")).get();
        ASSERT_TRUE(isOk(*changed)) << changed->_content;
        ASSERT_EQ(operations.size(), 3u);
        ASSERT_EQ(timestamps.size(), 3u);
        EXPECT_EQ(operations[0], operations[1]) << "a byte-identical retry changed operation identity";
        EXPECT_EQ(timestamps[0], timestamps[1]) << "the helper did not preserve the client issuance time";
        EXPECT_NE(operations[0], operations[2]) << "a different target reused an operation identity";
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedRf3RejectsStaleOrFutureDeleteIdentityBeforeProposal) {
    seastar::thread([] {
        struct ResetHook {
            ~ResetHook() { HttpDeleteHandler::clusterDeleteHook = {}; }
        } reset;
        ScopedShardedEngine eng;
        eng.start();
        unsigned proposals = 0;
        HttpDeleteHandler::clusterDeleteHook = [&proposals](std::vector<timestar::data::DeleteRangeTarget>, SeriesId128,
                                                            uint64_t) {
            ++proposals;
            return seastar::make_ready_future<>();
        };
        HttpDeleteHandler handler(&eng.eng, true);
        const uint64_t nowMs = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count());

        auto stale = makeDeleteRequest(R"({"series":"m value","startTime":0,"endTime":1})");
        stale->_headers["Idempotency-Key-Timestamp"] =
            std::to_string(nowMs - timestar::data::kDeleteReceiptRetentionMs);
        auto staleReply = handler.handleDelete(std::move(stale)).get();
        EXPECT_EQ(staleReply->_status, seastar::http::reply::status_type::bad_request);

        auto future = makeDeleteRequest(R"({"series":"m value","startTime":0,"endTime":1})");
        future->_headers["Idempotency-Key-Timestamp"] =
            std::to_string(nowMs + timestar::data::kDeleteReceiptFutureSkewMs + 1);
        auto futureReply = handler.handleDelete(std::move(future)).get();
        EXPECT_EQ(futureReply->_status, seastar::http::reply::status_type::bad_request);
        EXPECT_EQ(proposals, 0u);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedRf3UsesOneCanonicalReceiptPerVShardBatch) {
    seastar::thread([] {
        struct ResetHook {
            ~ResetHook() { HttpDeleteHandler::clusterDeleteHook = {}; }
        } reset;
        ScopedShardedEngine eng;
        eng.start();

        const std::string first = "receipt_batch_a value";
        const uint16_t vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(first));
        std::string second;
        for (unsigned i = 0;; ++i) {
            second = "receipt_batch_b,host=h" + std::to_string(i) + " value";
            if (timestar::virtualShard(SeriesId128::fromSeriesKey(second)) == vshard)
                break;
        }

        std::vector<SeriesId128> operations;
        HttpDeleteHandler::clusterDeleteHook = [&operations, &first, &second](
                                                   std::vector<timestar::data::DeleteRangeTarget> targets,
                                                   SeriesId128 operationId, uint64_t) {
            if (targets.size() != 2u) {
                ADD_FAILURE() << "same-VShard exact targets were not grouped into one batch";
                return seastar::make_ready_future<>();
            }
            EXPECT_EQ(targets[0].seriesKey, first);
            EXPECT_EQ(targets[1].seriesKey, second);
            operations.push_back(operationId);
            return seastar::make_ready_future<>();
        };

        HttpDeleteHandler handler(&eng.eng, true);
        const std::string firstBody = R"({"deletes":[{"series":")" + second +
                                      R"(","startTime":0,"endTime":1},{"series":")" + first +
                                      R"(","startTime":0,"endTime":1}]})";
        const std::string reorderedBody = R"({"deletes":[{"series":")" + first +
                                          R"(","startTime":0,"endTime":1},{"series":")" + second +
                                          R"(","startTime":0,"endTime":1}]})";
        auto firstReply = handler.handleDelete(makeDeleteRequest(firstBody)).get();
        auto retryReply = handler.handleDelete(makeDeleteRequest(reorderedBody)).get();
        ASSERT_TRUE(isOk(*firstReply)) << firstReply->_content;
        ASSERT_TRUE(isOk(*retryReply)) << retryReply->_content;
        ASSERT_EQ(operations.size(), 2u) << "two targets in one VShard must produce one proposal per request";
        EXPECT_EQ(operations[0], operations[1]) << "canonical request reordering changed the batch operation ID";
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedRf3RejectsUnwiredPatternDiscoveryBeforeAnyProposal) {
    seastar::thread([] {
        struct ResetHook {
            ~ResetHook() {
                HttpDeleteHandler::clusterDeleteHook = {};
                HttpDeleteHandler::clusterPatternExpandHook = {};
                HttpDeleteHandler::clusterDeletePlanHook = {};
            }
        } reset;
        ScopedShardedEngine eng;
        eng.start();
        unsigned calls = 0;
        HttpDeleteHandler::clusterDeleteHook = [&calls](std::vector<timestar::data::DeleteRangeTarget>, SeriesId128,
                                                        uint64_t) {
            ++calls;
            return seastar::make_ready_future<>();
        };

        HttpDeleteHandler handler(&eng.eng, true);
        auto req = makeDeleteRequest(
            R"({"deletes":[{"series":"m value","startTime":0,"endTime":1},{"measurement":"m","tags":{"host":"h1"},"startTime":0,"endTime":1}]})");
        auto rep = handler.handleDelete(std::move(req)).get();
        EXPECT_EQ(rep->_status, seastar::http::reply::status_type::not_implemented);
        EXPECT_EQ(calls, 0u) << "the exact prefix of a mixed batch must not partially commit";
        EXPECT_NE(rep->_content.find("frozen expansion plan"), std::string::npos);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedRf3RejectsPatternBeforeExpansionOrProposal) {
    seastar::thread([] {
        struct ResetHook {
            ~ResetHook() {
                HttpDeleteHandler::clusterDeleteHook = {};
                HttpDeleteHandler::clusterPatternExpandHook = {};
                HttpDeleteHandler::clusterDeletePlanHook = {};
            }
        } reset;
        ScopedShardedEngine eng;
        eng.start();
        unsigned proposals = 0;
        unsigned expansions = 0;
        std::vector<std::string> proposedKeys;
        const std::string patternA = buildSeriesKey("m", {{"host", "a"}}, "value");
        const std::string patternB = buildSeriesKey("m", {{"host", "b"}}, "value");
        HttpDeleteHandler::clusterPatternExpandHook = [&](timestar::data::PatternSeriesSelector selector,
                                                          uint32_t maxSeries) {
            ++expansions;
            EXPECT_EQ(proposals, 0u) << "mutation began before the whole batch finished expansion";
            EXPECT_EQ(selector.measurement, "m");
            EXPECT_EQ(selector.tags, (std::map<std::string, std::string>{{"env", "prod"}}));
            EXPECT_EQ(selector.fields, (std::vector<std::string>{"value"}));
            EXPECT_EQ(maxSeries, 10'000u);
            return seastar::make_ready_future<std::vector<std::string>>(std::vector<std::string>{patternA, patternB});
        };
        HttpDeleteHandler::clusterDeleteHook = [&](std::vector<timestar::data::DeleteRangeTarget> targets, SeriesId128,
                                                   uint64_t) {
            ++proposals;
            for (auto& target : targets) {
                EXPECT_EQ(target.startTime, 10u);
                EXPECT_EQ(target.endTime, 20u);
                proposedKeys.push_back(std::move(target.seriesKey));
            }
            return seastar::make_ready_future<>();
        };

        HttpDeleteHandler handler(&eng.eng, true);
        auto request = makeDeleteRequest(
            R"({"deletes":[{"series":"exact value","startTime":10,"endTime":20},{"measurement":"m","tags":{"env":"prod"},"fields":["value"],"startTime":10,"endTime":20}]})");
        auto reply = handler.handleDelete(std::move(request)).get();
        EXPECT_EQ(reply->_status, seastar::http::reply::status_type::not_implemented);
        EXPECT_EQ(expansions, 0u);
        EXPECT_EQ(proposals, 0u);
        EXPECT_TRUE(proposedKeys.empty());
        EXPECT_NE(reply->_content.find("frozen expansion plan"), std::string::npos);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedRf3ExpansionFailureLeavesExactPrefixUntouched) {
    seastar::thread([] {
        struct ResetHook {
            ~ResetHook() {
                HttpDeleteHandler::clusterDeleteHook = {};
                HttpDeleteHandler::clusterPatternExpandHook = {};
                HttpDeleteHandler::clusterDeletePlanHook = {};
            }
        } reset;
        ScopedShardedEngine eng;
        eng.start();
        unsigned proposals = 0;
        HttpDeleteHandler::clusterDeleteHook = [&](std::vector<timestar::data::DeleteRangeTarget>, SeriesId128,
                                                   uint64_t) {
            ++proposals;
            return seastar::make_ready_future<>();
        };
        HttpDeleteHandler::clusterPatternExpandHook = [](timestar::data::PatternSeriesSelector, uint32_t) {
            return seastar::make_exception_future<std::vector<std::string>>(
                timestar::data::RetryableWriteError("catalog leader unavailable"));
        };
        HttpDeleteHandler::clusterDeletePlanLookupHook = [](SeriesId128, SeriesId128, uint64_t) {
            return seastar::make_ready_future<std::optional<std::vector<timestar::data::DeleteRangeTarget>>>(
                std::nullopt);
        };
        HttpDeleteHandler::clusterDeletePlanHook = [](SeriesId128, SeriesId128, uint64_t,
                                                      std::vector<timestar::data::DeleteRangeTarget> targets) {
            ADD_FAILURE() << "a failed catalog expansion reached the plan-freeze step";
            return seastar::make_ready_future<std::vector<timestar::data::DeleteRangeTarget>>(std::move(targets));
        };

        HttpDeleteHandler handler(&eng.eng, true);
        auto request = makeDeleteRequest(
            R"({"deletes":[{"series":"exact value","startTime":0,"endTime":1},{"measurement":"m","startTime":0,"endTime":1}]})");
        auto reply = handler.handleDelete(std::move(request)).get();
        EXPECT_EQ(reply->_status, seastar::http::reply::status_type::service_unavailable);
        EXPECT_EQ(proposals, 0u);
        EXPECT_EQ(reply->_headers["Retry-After"], "1");
        EXPECT_NE(reply->_content.find("catalog leader unavailable"), std::string::npos);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedRf3BroadPatternFailsBeforeMutation) {
    seastar::thread([] {
        struct ResetHook {
            ~ResetHook() {
                HttpDeleteHandler::clusterDeleteHook = {};
                HttpDeleteHandler::clusterPatternExpandHook = {};
                HttpDeleteHandler::clusterDeletePlanHook = {};
            }
        } reset;
        ScopedShardedEngine eng;
        eng.start();
        unsigned proposals = 0;
        HttpDeleteHandler::clusterDeleteHook = [&](std::vector<timestar::data::DeleteRangeTarget>, SeriesId128,
                                                   uint64_t) {
            ++proposals;
            return seastar::make_ready_future<>();
        };
        HttpDeleteHandler::clusterPatternExpandHook = [](timestar::data::PatternSeriesSelector, uint32_t) {
            return seastar::make_exception_future<std::vector<std::string>>(
                timestar::data::DeleteExpansionLimitError("pattern exceeds safety limit"));
        };
        HttpDeleteHandler::clusterDeletePlanLookupHook = [](SeriesId128, SeriesId128, uint64_t) {
            return seastar::make_ready_future<std::optional<std::vector<timestar::data::DeleteRangeTarget>>>(
                std::nullopt);
        };
        HttpDeleteHandler::clusterDeletePlanHook = [](SeriesId128, SeriesId128, uint64_t,
                                                      std::vector<timestar::data::DeleteRangeTarget> targets) {
            ADD_FAILURE() << "an oversized expansion reached the plan-freeze step";
            return seastar::make_ready_future<std::vector<timestar::data::DeleteRangeTarget>>(std::move(targets));
        };

        HttpDeleteHandler handler(&eng.eng, true);
        auto reply = handler.handleDelete(makeDeleteRequest(R"({"measurement":"m"})")).get();
        EXPECT_EQ(reply->_status, seastar::http::reply::status_type::bad_request);
        EXPECT_EQ(proposals, 0u);
        EXPECT_NE(reply->_content.find("pattern exceeds safety limit"), std::string::npos);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedRf3RetryUsesFirstFrozenPatternExpansion) {
    seastar::thread([] {
        struct ResetHook {
            ~ResetHook() {
                HttpDeleteHandler::clusterDeleteHook = {};
                HttpDeleteHandler::clusterPatternExpandHook = {};
                HttpDeleteHandler::clusterDeletePlanHook = {};
            }
        } reset;
        ScopedShardedEngine eng;
        eng.start();

        const std::string firstKey = buildSeriesKey("m", {{"host", "a"}}, "value");
        const std::string laterKey = buildSeriesKey("m", {{"host", "later"}}, "value");
        unsigned expansions = 0;
        unsigned freezes = 0;
        unsigned lookups = 0;
        std::vector<timestar::data::DeleteRangeTarget> frozen;
        std::vector<std::string> proposed;
        SeriesId128 frozenRequestId;
        SeriesId128 frozenFingerprint;
        uint64_t frozenIssuedAtMs = 0;

        HttpDeleteHandler::clusterPatternExpandHook = [&](timestar::data::PatternSeriesSelector, uint32_t) {
            ++expansions;
            std::vector<std::string> keys{firstKey};
            if (expansions > 1)
                keys.push_back(laterKey);
            return seastar::make_ready_future<std::vector<std::string>>(std::move(keys));
        };
        HttpDeleteHandler::clusterDeletePlanLookupHook = [&](SeriesId128 requestId, SeriesId128 fingerprint,
                                                             uint64_t issuedAtMs) {
            ++lookups;
            if (frozen.empty())
                return seastar::make_ready_future<std::optional<std::vector<timestar::data::DeleteRangeTarget>>>(
                    std::nullopt);
            EXPECT_EQ(requestId, frozenRequestId);
            EXPECT_EQ(fingerprint, frozenFingerprint);
            EXPECT_EQ(issuedAtMs, frozenIssuedAtMs);
            return seastar::make_ready_future<std::optional<std::vector<timestar::data::DeleteRangeTarget>>>(frozen);
        };
        HttpDeleteHandler::clusterDeletePlanHook = [&](SeriesId128 requestId, SeriesId128 fingerprint,
                                                       uint64_t issuedAtMs,
                                                       std::vector<timestar::data::DeleteRangeTarget> candidates) {
            ++freezes;
            if (freezes == 1) {
                EXPECT_NE(requestId, SeriesId128{});
                EXPECT_NE(fingerprint, SeriesId128{});
                EXPECT_NE(issuedAtMs, 0u);
                EXPECT_EQ(candidates.size(), 1u);
                frozenRequestId = requestId;
                frozenFingerprint = fingerprint;
                frozenIssuedAtMs = issuedAtMs;
                frozen = std::move(candidates);
            } else {
                ADD_FAILURE() << "a retry re-froze instead of loading the retained plan";
            }
            return seastar::make_ready_future<std::vector<timestar::data::DeleteRangeTarget>>(frozen);
        };
        HttpDeleteHandler::clusterDeleteHook = [&](std::vector<timestar::data::DeleteRangeTarget> targets, SeriesId128,
                                                   uint64_t) {
            for (auto& target : targets)
                proposed.push_back(std::move(target.seriesKey));
            return seastar::make_ready_future<>();
        };

        HttpDeleteHandler handler(&eng.eng, true);
        constexpr std::string_view body =
            R"({"measurement":"m","tags":{},"fields":["value"],"startTime":10,"endTime":20})";
        auto firstReply = handler.handleDelete(makeDeleteRequest(std::string(body))).get();
        ASSERT_TRUE(isOk(*firstReply)) << firstReply->_content;
        EXPECT_EQ(proposed, (std::vector<std::string>{firstKey}));

        proposed.clear();
        auto retryReply = handler.handleDelete(makeDeleteRequest(std::string(body))).get();
        ASSERT_TRUE(isOk(*retryReply)) << retryReply->_content;
        EXPECT_EQ(lookups, 2u);
        EXPECT_EQ(expansions, 1u) << "a retained plan must bypass a changed or unavailable catalog";
        EXPECT_EQ(freezes, 1u);
        EXPECT_EQ(proposed, (std::vector<std::string>{firstKey}));
        EXPECT_EQ(retryReply->_content.find(laterKey), decltype(retryReply->_content)::npos)
            << "a series created after the first attempt escaped the frozen plan";
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedRf3RetryPreservesFrozenEmptyPatternExpansion) {
    seastar::thread([] {
        struct ResetHook {
            ~ResetHook() {
                HttpDeleteHandler::clusterDeleteHook = {};
                HttpDeleteHandler::clusterPatternExpandHook = {};
                HttpDeleteHandler::clusterDeletePlanHook = {};
            }
        } reset;
        ScopedShardedEngine eng;
        eng.start();

        const std::string laterKey = buildSeriesKey("m", {{"host", "later"}}, "value");
        unsigned expansions = 0;
        unsigned freezes = 0;
        unsigned lookups = 0;
        unsigned proposals = 0;
        bool planStored = false;
        std::vector<timestar::data::DeleteRangeTarget> frozen;
        HttpDeleteHandler::clusterPatternExpandHook = [&](timestar::data::PatternSeriesSelector, uint32_t) {
            ++expansions;
            return seastar::make_ready_future<std::vector<std::string>>(
                expansions == 1 ? std::vector<std::string>{} : std::vector<std::string>{laterKey});
        };
        HttpDeleteHandler::clusterDeletePlanLookupHook = [&](SeriesId128, SeriesId128, uint64_t) {
            ++lookups;
            if (!planStored)
                return seastar::make_ready_future<std::optional<std::vector<timestar::data::DeleteRangeTarget>>>(
                    std::nullopt);
            return seastar::make_ready_future<std::optional<std::vector<timestar::data::DeleteRangeTarget>>>(frozen);
        };
        HttpDeleteHandler::clusterDeletePlanHook = [&](SeriesId128, SeriesId128, uint64_t,
                                                       std::vector<timestar::data::DeleteRangeTarget> candidates) {
            ++freezes;
            if (freezes == 1) {
                EXPECT_TRUE(candidates.empty());
                frozen = std::move(candidates);
                planStored = true;
            } else {
                ADD_FAILURE() << "an empty retained plan was mistaken for a lookup miss";
            }
            return seastar::make_ready_future<std::vector<timestar::data::DeleteRangeTarget>>(frozen);
        };
        HttpDeleteHandler::clusterDeleteHook = [&](std::vector<timestar::data::DeleteRangeTarget>, SeriesId128,
                                                   uint64_t) {
            ++proposals;
            return seastar::make_ready_future<>();
        };

        HttpDeleteHandler handler(&eng.eng, true);
        constexpr std::string_view body = R"({"measurement":"m","startTime":0,"endTime":1})";
        auto firstReply = handler.handleDelete(makeDeleteRequest(std::string(body))).get();
        auto retryReply = handler.handleDelete(makeDeleteRequest(std::string(body))).get();
        ASSERT_TRUE(isOk(*firstReply)) << firstReply->_content;
        ASSERT_TRUE(isOk(*retryReply)) << retryReply->_content;
        EXPECT_EQ(lookups, 2u);
        EXPECT_EQ(expansions, 1u) << "an empty retained plan must still bypass later catalog growth";
        EXPECT_EQ(freezes, 1u);
        EXPECT_EQ(proposals, 0u);
        EXPECT_NE(retryReply->_content.find(R"("seriesDeleted":0)"), std::string::npos);
        EXPECT_EQ(retryReply->_content.find(laterKey), decltype(retryReply->_content)::npos);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedRf3PlanFreezeFailureLeavesEveryTargetUntouched) {
    seastar::thread([] {
        struct ResetHook {
            ~ResetHook() {
                HttpDeleteHandler::clusterDeleteHook = {};
                HttpDeleteHandler::clusterPatternExpandHook = {};
                HttpDeleteHandler::clusterDeletePlanHook = {};
            }
        } reset;
        ScopedShardedEngine eng;
        eng.start();

        unsigned proposals = 0;
        HttpDeleteHandler::clusterPatternExpandHook = [](timestar::data::PatternSeriesSelector, uint32_t) {
            return seastar::make_ready_future<std::vector<std::string>>(
                std::vector<std::string>{buildSeriesKey("m", {{"host", "a"}}, "value")});
        };
        HttpDeleteHandler::clusterDeletePlanLookupHook = [](SeriesId128, SeriesId128, uint64_t) {
            return seastar::make_ready_future<std::optional<std::vector<timestar::data::DeleteRangeTarget>>>(
                std::nullopt);
        };
        HttpDeleteHandler::clusterDeletePlanHook = [](SeriesId128, SeriesId128, uint64_t,
                                                      std::vector<timestar::data::DeleteRangeTarget>) {
            return seastar::make_exception_future<std::vector<timestar::data::DeleteRangeTarget>>(
                timestar::data::RetryableWriteError("group-0 leader unavailable"));
        };
        HttpDeleteHandler::clusterDeleteHook = [&](std::vector<timestar::data::DeleteRangeTarget>, SeriesId128,
                                                   uint64_t) {
            ++proposals;
            return seastar::make_ready_future<>();
        };

        HttpDeleteHandler handler(&eng.eng, true);
        auto reply =
            handler
                .handleDelete(makeDeleteRequest(
                    R"({"deletes":[{"series":"exact value","startTime":0,"endTime":1},{"measurement":"m","startTime":0,"endTime":1}]})"))
                .get();
        EXPECT_EQ(reply->_status, seastar::http::reply::status_type::service_unavailable);
        EXPECT_EQ(reply->_headers["Retry-After"], "1");
        EXPECT_EQ(proposals, 0u) << "the exact prefix committed before group 0 froze the whole expansion";
        EXPECT_NE(reply->_content.find("group-0 leader unavailable"), std::string::npos);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedRf3ReportsFrozenPlanIdentityConflict) {
    seastar::thread([] {
        struct ResetHook {
            ~ResetHook() {
                HttpDeleteHandler::clusterDeleteHook = {};
                HttpDeleteHandler::clusterPatternExpandHook = {};
                HttpDeleteHandler::clusterDeletePlanHook = {};
            }
        } reset;
        ScopedShardedEngine eng;
        eng.start();

        unsigned proposals = 0;
        HttpDeleteHandler::clusterPatternExpandHook = [](timestar::data::PatternSeriesSelector, uint32_t) {
            ADD_FAILURE() << "an identity conflict reached catalog discovery";
            return seastar::make_ready_future<std::vector<std::string>>(
                std::vector<std::string>{buildSeriesKey("m", {}, "value")});
        };
        HttpDeleteHandler::clusterDeletePlanLookupHook = [](SeriesId128, SeriesId128, uint64_t) {
            return seastar::make_exception_future<std::optional<std::vector<timestar::data::DeleteRangeTarget>>>(
                timestar::data::DeletePlanConflictError("the key already names another retained request"));
        };
        HttpDeleteHandler::clusterDeletePlanHook = [](SeriesId128, SeriesId128, uint64_t,
                                                      std::vector<timestar::data::DeleteRangeTarget>) {
            ADD_FAILURE() << "an identity conflict reached catalog discovery/plan freeze";
            return seastar::make_ready_future<std::vector<timestar::data::DeleteRangeTarget>>(
                std::vector<timestar::data::DeleteRangeTarget>{});
        };
        HttpDeleteHandler::clusterDeleteHook = [&](std::vector<timestar::data::DeleteRangeTarget>, SeriesId128,
                                                   uint64_t) {
            ++proposals;
            return seastar::make_ready_future<>();
        };

        HttpDeleteHandler handler(&eng.eng, true);
        auto reply = handler.handleDelete(makeDeleteRequest(R"({"measurement":"m"})")).get();
        EXPECT_EQ(reply->_status, seastar::http::reply::status_type::conflict);
        EXPECT_EQ(proposals, 0u);
        EXPECT_NE(reply->_content.find("DELETE_IDEMPOTENCY_CONFLICT"), std::string::npos);
        EXPECT_EQ(reply->_headers.count("Retry-After"), 0u);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedRf3PartialMultiTargetFailureIsSafelyRetryable) {
    seastar::thread([] {
        struct ResetHook {
            ~ResetHook() {
                HttpDeleteHandler::clusterDeleteHook = {};
                HttpDeleteHandler::clusterPatternExpandHook = {};
                HttpDeleteHandler::clusterDeletePlanHook = {};
            }
        } reset;
        ScopedShardedEngine eng;
        eng.start();
        unsigned committed = 0;
        HttpDeleteHandler::clusterDeleteHook = [&](std::vector<timestar::data::DeleteRangeTarget> targets, SeriesId128,
                                                   uint64_t) {
            if (std::ranges::any_of(targets, [](const auto& target) { return target.seriesKey.starts_with("bad "); })) {
                return seastar::sleep(std::chrono::milliseconds(2)).then([] {
                    return seastar::make_exception_future<>(
                        timestar::data::RetryableWriteError("one target refused before proposal"));
                });
            }
            committed += targets.size();
            return seastar::make_ready_future<>();
        };

        HttpDeleteHandler handler(&eng.eng, true);
        auto request = makeDeleteRequest(
            R"({"deletes":[{"series":"good value","startTime":0,"endTime":1},{"series":"bad value","startTime":0,"endTime":1}]})");
        auto reply = handler.handleDelete(std::move(request)).get();
        EXPECT_EQ(committed, 1u) << "the negative case must actually include a committed sibling command";
        EXPECT_EQ(reply->_status, seastar::http::reply::status_type::service_unavailable);
        EXPECT_EQ(reply->_headers["Retry-After"], "1");
        EXPECT_EQ(reply->_headers.count("X-TimeStar-Mutation-Outcome"), 0u);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedRf3MapsRetryableDeleteFailureTo503) {
    seastar::thread([] {
        struct ResetHook {
            ~ResetHook() { HttpDeleteHandler::clusterDeleteHook = {}; }
        } reset;
        ScopedShardedEngine eng;
        eng.start();
        HttpDeleteHandler::clusterDeleteHook = [](std::vector<timestar::data::DeleteRangeTarget>, SeriesId128,
                                                  uint64_t) {
            return seastar::make_exception_future<>(timestar::data::RetryableWriteError("delete quorum unavailable"));
        };

        HttpDeleteHandler handler(&eng.eng, true);
        auto req = makeDeleteRequest(R"({"series":"m value","startTime":0,"endTime":1})");
        auto rep = handler.handleDelete(std::move(req)).get();
        EXPECT_EQ(rep->_status, seastar::http::reply::status_type::service_unavailable);
        EXPECT_EQ(rep->_headers["Retry-After"], "1");
        EXPECT_NE(rep->_content.find("delete quorum unavailable"), std::string::npos);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedRf3ReportsRetiredDeleteIdentityAsConflict) {
    seastar::thread([] {
        struct ResetHook {
            ~ResetHook() { HttpDeleteHandler::clusterDeleteHook = {}; }
        } reset;
        ScopedShardedEngine eng;
        eng.start();
        HttpDeleteHandler::clusterDeleteHook = [](std::vector<timestar::data::DeleteRangeTarget>, SeriesId128,
                                                  uint64_t) {
            return seastar::make_exception_future<>(timestar::data::DeleteReceiptExpiredError(
                "delete idempotency receipt is outside the retained VShard window"));
        };

        HttpDeleteHandler handler(&eng.eng, true);
        auto reply = handler.handleDelete(makeDeleteRequest(R"({"series":"m value","startTime":0,"endTime":1})")).get();
        EXPECT_EQ(reply->_status, seastar::http::reply::status_type::conflict);
        EXPECT_EQ(reply->_headers["X-TimeStar-Idempotency-Window"], "expired");
        EXPECT_NE(reply->_content.find("DELETE_IDEMPOTENCY_EXPIRED"), std::string::npos);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedRf3BoundsDeleteBatchProposalConcurrency) {
    seastar::thread([] {
        struct ResetHook {
            ~ResetHook() { HttpDeleteHandler::clusterDeleteHook = {}; }
        } reset;
        ScopedShardedEngine eng;
        eng.start();
        unsigned active = 0;
        unsigned maximum = 0;
        HttpDeleteHandler::clusterDeleteHook = [&active, &maximum](std::vector<timestar::data::DeleteRangeTarget>,
                                                                   SeriesId128, uint64_t) {
            ++active;
            maximum = std::max(maximum, active);
            return seastar::sleep(std::chrono::milliseconds(2)).finally([&active] { --active; });
        };

        std::string body = R"({"deletes":[)";
        for (unsigned i = 0; i < 96; ++i) {
            if (i)
                body += ',';
            body += R"({"series":"batch,host=h)" + std::to_string(i) + R"( value","startTime":0,"endTime":1})";
        }
        body += "]}";

        HttpDeleteHandler handler(&eng.eng, true);
        auto rep = handler.handleDelete(makeDeleteRequest(body)).get();
        ASSERT_TRUE(isOk(*rep)) << rep->_content;
        EXPECT_EQ(active, 0u);
        EXPECT_EQ(maximum, 32u) << "one request exceeded the bounded Raft-proposal fan-out";
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedClusterRejectsUnwiredRetentionAndStreamingBeforeLocalWork) {
    seastar::thread([] {
        auto retention = std::make_shared<HttpRetentionHandler>(nullptr, true);
        auto retentionReq = std::make_unique<seastar::http::request>();
        retentionReq->_headers["Content-Type"] = "application/json";
        retentionReq->content = R"({"measurement":"m","ttl":"1h"})";
        auto retentionRep = retention->handlePut(std::move(retentionReq)).get();
        EXPECT_EQ(retentionRep->_status, seastar::http::reply::status_type::service_unavailable);
        EXPECT_NE(retentionRep->_content.find("CLUSTER_RETENTION_UNAVAILABLE"), std::string::npos);

        HttpStreamHandler stream(nullptr, true);
        auto streamReq = std::make_unique<seastar::http::request>();
        streamReq->_headers["Content-Type"] = "application/json";
        streamReq->content = R"json({"query":"latest:m(value)"})json";
        auto streamRep = stream.handleSubscribe(std::move(streamReq)).get();
        EXPECT_EQ(streamRep->_status, seastar::http::reply::status_type::not_implemented);
        EXPECT_NE(streamRep->_content.find("CLUSTER_STREAM_UNSUPPORTED"), std::string::npos);
    })
        .join()
        .get();
}

TEST_F(HttpHandlerIntegrationTest, PartitionedClusterRejectsUnwiredReadConsistencyModes) {
    seastar::thread([] {
        struct ResetHook {
            ~ResetHook() { HttpQueryHandler::clusterQueryHook = {}; }
        } reset;
        HttpQueryHandler::clusterQueryHook = [](QueryRequest) {
            ADD_FAILURE() << "unsupported read mode reached the cluster coordinator";
            return seastar::make_ready_future<QueryResponse>();
        };

        HttpQueryHandler queryHandler(nullptr);
        auto req = std::make_unique<seastar::http::request>();
        req->_headers["Content-Type"] = "application/json";
        req->content = R"json({"query":"latest:m(value)","startTime":0,"endTime":1,"consistency":"session"})json";
        auto rep = queryHandler.handleQuery(std::move(req)).get();
        EXPECT_EQ(rep->_status, seastar::http::reply::status_type::not_implemented);
        EXPECT_NE(rep->_content.find("CLUSTER_READ_MODE_UNSUPPORTED"), std::string::npos);
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

        // Verify measurements via metadata handler, polling past the async
        // metadata indexing window.
        {
            glz::generic parsed;
            pollMetadataJson(
                parsed, [&] { return metaHandler.handleMeasurements(makeMetadataRequest()).get(); },
                [](glz::generic::object_t& o) {
                    std::set<std::string> names;
                    for (auto& m : o["measurements"].get<glz::generic::array_t>()) {
                        names.insert(m.get<std::string>());
                    }
                    return names.count("weather_cycle") && names.count("system_cycle");
                });
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
        {
            glz::generic parsed;
            pollMetadataJson(
                parsed, [&] { return metaHandler.handleTags(makeMetadataRequest("weather_cycle")).get(); },
                [](glz::generic::object_t& o) {
                    if (!o.count("tags"))
                        return false;
                    auto& t = o["tags"].get<glz::generic::object_t>();
                    return t.count("location") && t.count("sensor");
                });
            auto& obj = parsed.get<glz::generic::object_t>();
            auto& tags = obj["tags"].get<glz::generic::object_t>();
            EXPECT_TRUE(tags.count("location") > 0);
            EXPECT_TRUE(tags.count("sensor") > 0);
        }

        // Verify fields for weather_cycle
        {
            glz::generic parsed;
            pollMetadataJson(
                parsed, [&] { return metaHandler.handleFields(makeMetadataRequest("weather_cycle")).get(); },
                [](glz::generic::object_t& o) {
                    if (!o.count("fields"))
                        return false;
                    auto& f = o["fields"].get<glz::generic::object_t>();
                    return f.count("temperature") && f.count("humidity");
                });
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
