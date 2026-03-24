// Integration tests for protobuf wire format through the HTTP handler stack.
//
// These tests exercise the full pipeline: build protobuf request bytes ->
// set Content-Type/Accept headers -> invoke handler coroutine -> parse
// protobuf (or JSON) response -> verify data correctness.
//
// This file is built as part of a SEPARATE test binary (timestar_proto_integration_test)
// because it includes timestar.pb.h to construct request messages.  The proto
// package is "timestar_pb" (no collision with the internal timestar:: namespace),
// but linking against timestar_proto requires explicit target dependency.
//
// Pattern follows HttpHandlerIntegrationTest in http_handler_integration_test.cpp.

#include "proto_converters.hpp"
#include "timestar.pb.h"

#include <glaze/glaze.hpp>

#include <gtest/gtest.h>

#include <cmath>
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
#include "../../../lib/http/http_delete_handler.hpp"
#include "../../../lib/http/http_metadata_handler.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/http/http_write_handler.hpp"

using namespace timestar;
namespace fs = std::filesystem;

// ============================================================================
// Helpers to build protobuf request bodies
// ============================================================================

static std::string buildProtoWriteRequest(const std::string& measurement,
                                          const std::map<std::string, std::string>& tags,
                                          const std::map<std::string, std::vector<double>>& doubleFields,
                                          const std::vector<uint64_t>& timestamps) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement(measurement);
    for (auto& [k, v] : tags) {
        (*wp->mutable_tags())[k] = v;
    }
    for (auto ts : timestamps) {
        wp->add_timestamps(ts);
    }
    for (auto& [fieldName, values] : doubleFields) {
        ::timestar_pb::WriteField wf;
        for (auto v : values) {
            wf.mutable_double_values()->add_values(v);
        }
        (*wp->mutable_fields())[fieldName] = wf;
    }
    std::string bytes;
    req.SerializeToString(&bytes);
    return bytes;
}

static std::string buildProtoBatchWriteRequest(
    const std::vector<std::tuple<std::string,                               // measurement
                                 std::map<std::string, std::string>,        // tags
                                 std::map<std::string, std::vector<double>>,// double fields
                                 std::vector<uint64_t>                      // timestamps
                                 >>& points) {
    ::timestar_pb::WriteRequest req;
    for (auto& [meas, tags, fields, timestamps] : points) {
        auto* wp = req.add_writes();
        wp->set_measurement(meas);
        for (auto& [k, v] : tags) {
            (*wp->mutable_tags())[k] = v;
        }
        for (auto ts : timestamps) {
            wp->add_timestamps(ts);
        }
        for (auto& [fieldName, values] : fields) {
            ::timestar_pb::WriteField wf;
            for (auto v : values) {
                wf.mutable_double_values()->add_values(v);
            }
            (*wp->mutable_fields())[fieldName] = wf;
        }
    }
    std::string bytes;
    req.SerializeToString(&bytes);
    return bytes;
}

static std::string buildProtoQueryRequest(const std::string& query,
                                          uint64_t startTime, uint64_t endTime,
                                          const std::string& aggregationInterval = "") {
    ::timestar_pb::QueryRequest req;
    req.set_query(query);
    req.set_start_time(startTime);
    req.set_end_time(endTime);
    if (!aggregationInterval.empty()) {
        req.set_aggregation_interval(aggregationInterval);
    }
    std::string bytes;
    req.SerializeToString(&bytes);
    return bytes;
}

static std::string buildProtoDeleteRequest(const std::string& measurement,
                                           const std::map<std::string, std::string>& tags = {},
                                           const std::string& field = "",
                                           uint64_t startTime = 0,
                                           uint64_t endTime = UINT64_MAX) {
    ::timestar_pb::DeleteRequest req;
    req.set_measurement(measurement);
    for (auto& [k, v] : tags) {
        (*req.mutable_tags())[k] = v;
    }
    if (!field.empty()) {
        req.set_field(field);
    }
    req.set_start_time(startTime);
    req.set_end_time(endTime);
    std::string bytes;
    req.SerializeToString(&bytes);
    return bytes;
}

// ============================================================================
// Response parsing helpers
// ============================================================================

static ::timestar_pb::WriteResponse parseProtoWriteResponse(const std::string& bytes) {
    ::timestar_pb::WriteResponse resp;
    EXPECT_TRUE(resp.ParseFromString(bytes)) << "Failed to parse WriteResponse proto";
    return resp;
}

static ::timestar_pb::QueryResponse parseProtoQueryResponse(const std::string& bytes) {
    ::timestar_pb::QueryResponse resp;
    EXPECT_TRUE(resp.ParseFromString(bytes)) << "Failed to parse QueryResponse proto";
    return resp;
}

static ::timestar_pb::DeleteResponse parseProtoDeleteResponse(const std::string& bytes) {
    ::timestar_pb::DeleteResponse resp;
    EXPECT_TRUE(resp.ParseFromString(bytes)) << "Failed to parse DeleteResponse proto";
    return resp;
}

// ============================================================================
// Test fixture
// ============================================================================

class ProtobufIntegrationTest : public ::testing::Test {
protected:
    static inline fs::path expectedCwd;

    static void SetUpTestSuite() { expectedCwd = fs::current_path(); }

    void SetUp() override {
        if (fs::current_path() != expectedCwd && !expectedCwd.empty()) {
            fs::current_path(expectedCwd);
        }
        robustCleanShardDirectories();
    }

    void TearDown() override { robustCleanShardDirectories(); }

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

    static std::unique_ptr<seastar::http::request> makeProtoWriteRequest(const std::string& protoBytes) {
        auto req = std::make_unique<seastar::http::request>();
        req->content = protoBytes;
        req->_headers["Content-Type"] = "application/x-protobuf";
        req->_headers["Accept"] = "application/x-protobuf";
        return req;
    }

    static std::unique_ptr<seastar::http::request> makeProtoQueryRequest(const std::string& protoBytes) {
        auto req = std::make_unique<seastar::http::request>();
        req->content = protoBytes;
        req->_headers["Content-Type"] = "application/x-protobuf";
        req->_headers["Accept"] = "application/x-protobuf";
        return req;
    }

    static std::unique_ptr<seastar::http::request> makeProtoDeleteRequest(const std::string& protoBytes) {
        auto req = std::make_unique<seastar::http::request>();
        req->content = protoBytes;
        req->_headers["Content-Type"] = "application/x-protobuf";
        req->_headers["Accept"] = "application/x-protobuf";
        return req;
    }

    // Write with protobuf body but request JSON response
    static std::unique_ptr<seastar::http::request> makeProtoWriteJsonResponseRequest(const std::string& protoBytes) {
        auto req = std::make_unique<seastar::http::request>();
        req->content = protoBytes;
        req->_headers["Content-Type"] = "application/x-protobuf";
        req->_headers["Accept"] = "application/json";
        return req;
    }

    // Write with JSON body but request protobuf response
    static std::unique_ptr<seastar::http::request> makeJsonWriteRequest(const std::string& jsonBody) {
        auto req = std::make_unique<seastar::http::request>();
        req->content = jsonBody;
        req->_headers["Content-Type"] = "application/json";
        return req;
    }

    // Query with JSON body but request protobuf response
    static std::unique_ptr<seastar::http::request> makeJsonQueryProtoResponse(const std::string& jsonBody) {
        auto req = std::make_unique<seastar::http::request>();
        req->content = jsonBody;
        req->_headers["Content-Type"] = "application/json";
        req->_headers["Accept"] = "application/x-protobuf";
        return req;
    }

    // Metadata request with Accept: protobuf
    static std::unique_ptr<seastar::http::request> makeMetadataRequestProto(const std::string& measurement = "",
                                                                             const std::string& tag = "") {
        auto req = std::make_unique<seastar::http::request>();
        req->_headers["Accept"] = "application/x-protobuf";
        if (!measurement.empty()) {
            req->query_parameters["measurement"] = measurement;
        }
        if (!tag.empty()) {
            req->query_parameters["tag"] = tag;
        }
        return req;
    }

    static bool isOk(const seastar::http::reply& rep) { return rep._status == seastar::http::reply::status_type::ok; }
};

// ============================================================================
// 1. Protobuf Write Tests
// ============================================================================

TEST_F(ProtobufIntegrationTest, WriteSinglePointProto) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        auto protoBytes = buildProtoWriteRequest(
            "pb_single_temp",
            {{"location", "us-west"}},
            {{"value", {72.5}}},
            {1000000000ULL});

        auto req = makeProtoWriteRequest(protoBytes);
        auto rep = handler.handleWrite(std::move(req)).get();

        ASSERT_TRUE(isOk(*rep)) << "Reply body size: " << rep->_content.size();

        auto resp = parseProtoWriteResponse(rep->_content);
        EXPECT_EQ(resp.status(), "success");
        EXPECT_GE(resp.points_written(), 1);
        EXPECT_EQ(resp.failed_writes(), 0);
    })
        .join()
        .get();
}

TEST_F(ProtobufIntegrationTest, WriteBatchPointsProto) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        auto protoBytes = buildProtoBatchWriteRequest({
            {"pb_batch_cpu", {{"host", "server-01"}}, {{"usage", {45.2}}}, {1000000000ULL}},
            {"pb_batch_cpu", {{"host", "server-02"}}, {{"usage", {67.8}}}, {1000000000ULL}},
            {"pb_batch_mem", {{"host", "server-01"}}, {{"used_gb", {12.5}}}, {1000000000ULL}},
        });

        auto req = makeProtoWriteRequest(protoBytes);
        auto rep = handler.handleWrite(std::move(req)).get();

        ASSERT_TRUE(isOk(*rep));

        auto resp = parseProtoWriteResponse(rep->_content);
        EXPECT_EQ(resp.status(), "success");
        EXPECT_GE(resp.points_written(), 3);
    })
        .join()
        .get();
}

TEST_F(ProtobufIntegrationTest, WriteArrayValuesProto) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        auto protoBytes = buildProtoWriteRequest(
            "pb_array_sensor",
            {{"zone", "a"}},
            {{"temp", {20.0, 21.0, 22.0}}},
            {1000000000ULL, 2000000000ULL, 3000000000ULL});

        auto req = makeProtoWriteRequest(protoBytes);
        auto rep = handler.handleWrite(std::move(req)).get();

        ASSERT_TRUE(isOk(*rep));

        auto resp = parseProtoWriteResponse(rep->_content);
        EXPECT_EQ(resp.status(), "success");
        EXPECT_GE(resp.points_written(), 3);
    })
        .join()
        .get();
}

TEST_F(ProtobufIntegrationTest, WriteEmptyProtoBodyReturnsError) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        auto req = makeProtoWriteRequest("");
        auto rep = handler.handleWrite(std::move(req)).get();

        EXPECT_EQ(rep->_status, seastar::http::reply::status_type::bad_request);
    })
        .join()
        .get();
}

TEST_F(ProtobufIntegrationTest, WriteInvalidProtoBodyReturnsError) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        // Send garbage bytes as protobuf
        auto req = makeProtoWriteRequest("not-valid-protobuf-\x00\x01\x02");
        auto rep = handler.handleWrite(std::move(req)).get();

        // Should get an error (either bad_request or internal_server_error)
        auto status = static_cast<int>(rep->_status);
        EXPECT_TRUE(status == 400 || status == 500) << "Got HTTP " << status;
    })
        .join()
        .get();
}

// ============================================================================
// 2. Protobuf Write-then-Query Round-Trip Tests
// ============================================================================

TEST_F(ProtobufIntegrationTest, WriteProtoQueryProtoRoundTrip) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write via protobuf
        auto writeBytes = buildProtoWriteRequest(
            "pb_rt_temp",
            {{"location", "us-west"}},
            {{"value", {72.5}}},
            {1000000000ULL});

        auto writeReq = makeProtoWriteRequest(writeBytes);
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));
        auto writeResp = parseProtoWriteResponse(writeRep->_content);
        EXPECT_EQ(writeResp.status(), "success");

        // Query via protobuf
        auto queryBytes = buildProtoQueryRequest(
            "avg:pb_rt_temp(value){location:us-west}", 0, 2000000000ULL);

        auto queryReq = makeProtoQueryRequest(queryBytes);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep)) << "Query failed (HTTP " << static_cast<int>(queryRep->_status) << ")";

        auto queryResp = parseProtoQueryResponse(queryRep->_content);
        EXPECT_EQ(queryResp.status(), "success");
        EXPECT_GE(queryResp.series_size(), 1);

        if (queryResp.series_size() > 0) {
            auto& series = queryResp.series(0);
            EXPECT_EQ(series.measurement(), "pb_rt_temp");
            EXPECT_TRUE(series.fields().count("value") > 0);

            if (series.fields().count("value") > 0) {
                auto& fieldData = series.fields().at("value");
                EXPECT_GE(fieldData.timestamps_size(), 1);
                EXPECT_TRUE(fieldData.has_double_values());
                if (fieldData.has_double_values()) {
                    EXPECT_GE(fieldData.double_values().values_size(), 1);
                    EXPECT_NEAR(fieldData.double_values().values(0), 72.5, 0.001);
                }
            }
        }

        EXPECT_GE(queryResp.statistics().point_count(), 1u);
    })
        .join()
        .get();
}

TEST_F(ProtobufIntegrationTest, WriteProtoQueryProtoMultipleSeries) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write multiple series via protobuf
        auto writeBytes = buildProtoBatchWriteRequest({
            {"pb_multi_weather", {{"location", "us-west"}}, {{"temp", {72.5}}, {"humidity", {45.0}}}, {1000000000ULL}},
            {"pb_multi_weather", {{"location", "us-east"}}, {{"temp", {68.0}}, {"humidity", {55.0}}}, {1000000000ULL}},
            {"pb_multi_weather", {{"location", "us-west"}}, {{"temp", {73.0}}, {"humidity", {44.0}}}, {2000000000ULL}},
            {"pb_multi_weather", {{"location", "us-east"}}, {{"temp", {69.0}}, {"humidity", {54.0}}}, {2000000000ULL}},
        });

        auto writeReq = makeProtoWriteRequest(writeBytes);
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Query all weather data via protobuf
        auto queryBytes = buildProtoQueryRequest("avg:pb_multi_weather(temp)", 0, 3000000000ULL);
        auto queryReq = makeProtoQueryRequest(queryBytes);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep));

        auto queryResp = parseProtoQueryResponse(queryRep->_content);
        EXPECT_EQ(queryResp.status(), "success");
        EXPECT_GE(queryResp.statistics().series_count(), 1u);
        EXPECT_GE(queryResp.statistics().point_count(), 1u);
    })
        .join()
        .get();
}

TEST_F(ProtobufIntegrationTest, WriteProtoQueryWithTimeFilter) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write 5 data points via protobuf
        auto writeBytes = buildProtoWriteRequest(
            "pb_timefilter_metric",
            {{"host", "h1"}},
            {{"value", {10.0, 20.0, 30.0, 40.0, 50.0}}},
            {1000000000ULL, 2000000000ULL, 3000000000ULL, 4000000000ULL, 5000000000ULL});

        auto writeReq = makeProtoWriteRequest(writeBytes);
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Query only middle range via protobuf
        auto queryBytes = buildProtoQueryRequest(
            "avg:pb_timefilter_metric(value){host:h1}", 2000000000ULL, 4000000000ULL);
        auto queryReq = makeProtoQueryRequest(queryBytes);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep));

        auto queryResp = parseProtoQueryResponse(queryRep->_content);
        EXPECT_EQ(queryResp.status(), "success");
        EXPECT_GE(queryResp.statistics().point_count(), 1u);
    })
        .join()
        .get();
}

TEST_F(ProtobufIntegrationTest, WriteProtoQueryWithAggregationInterval) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        auto writeBytes = buildProtoWriteRequest(
            "pb_aggint_metric",
            {{"host", "h1"}},
            {{"value", {10.0, 15.0, 20.0, 25.0, 30.0, 35.0}}},
            {1000000000ULL, 1500000000ULL, 2000000000ULL, 2500000000ULL, 3000000000ULL, 3500000000ULL});

        auto writeReq = makeProtoWriteRequest(writeBytes);
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Query with 2-second aggregation interval via protobuf
        auto queryBytes = buildProtoQueryRequest(
            "avg:pb_aggint_metric(value){host:h1}", 0, 4000000000ULL, "2s");
        auto queryReq = makeProtoQueryRequest(queryBytes);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep));

        auto queryResp = parseProtoQueryResponse(queryRep->_content);
        EXPECT_EQ(queryResp.status(), "success");
        EXPECT_GE(queryResp.statistics().point_count(), 1u);
    })
        .join()
        .get();
}

TEST_F(ProtobufIntegrationTest, QueryMaxAggregationProto) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        auto writeBytes = buildProtoWriteRequest(
            "pb_sensor_max",
            {{"zone", "a"}},
            {{"temp", {10.0, 25.0, 15.0, 30.0, 20.0}}},
            {1000000000ULL, 2000000000ULL, 3000000000ULL, 4000000000ULL, 5000000000ULL});

        auto writeReq = makeProtoWriteRequest(writeBytes);
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Query with max aggregation via protobuf
        auto queryBytes = buildProtoQueryRequest("max:pb_sensor_max(temp){zone:a}", 0, 6000000000ULL, "10s");
        auto queryReq = makeProtoQueryRequest(queryBytes);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep));

        auto queryResp = parseProtoQueryResponse(queryRep->_content);
        EXPECT_EQ(queryResp.status(), "success");
        ASSERT_GE(queryResp.series_size(), 1);

        auto& series = queryResp.series(0);
        ASSERT_TRUE(series.fields().count("temp") > 0);
        auto& fieldData = series.fields().at("temp");
        ASSERT_TRUE(fieldData.has_double_values());
        ASSERT_GE(fieldData.double_values().values_size(), 1);
        EXPECT_NEAR(fieldData.double_values().values(0), 30.0, 0.001);
    })
        .join()
        .get();
}

// ============================================================================
// 3. Cross-Format Tests (Write JSON, Query Protobuf and vice versa)
// ============================================================================

TEST_F(ProtobufIntegrationTest, WriteJsonQueryProtobuf) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write via JSON
        auto jsonReq = std::make_unique<seastar::http::request>();
        jsonReq->content = R"({
            "measurement": "pb_cross_json2pb",
            "tags": {"location": "us-west"},
            "fields": {"value": 88.5},
            "timestamp": 1000000000
        })";
        jsonReq->_headers["Content-Type"] = "application/json";

        auto writeRep = writeHandler.handleWrite(std::move(jsonReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Query via protobuf request + protobuf response
        auto queryBytes = buildProtoQueryRequest(
            "avg:pb_cross_json2pb(value){location:us-west}", 0, 2000000000ULL);
        auto queryReq = makeProtoQueryRequest(queryBytes);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep));

        auto queryResp = parseProtoQueryResponse(queryRep->_content);
        EXPECT_EQ(queryResp.status(), "success");
        ASSERT_GE(queryResp.series_size(), 1);

        auto& series = queryResp.series(0);
        EXPECT_EQ(series.measurement(), "pb_cross_json2pb");
        EXPECT_TRUE(series.fields().count("value") > 0);
    })
        .join()
        .get();
}

TEST_F(ProtobufIntegrationTest, WriteProtobufQueryJson) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write via protobuf
        auto writeBytes = buildProtoWriteRequest(
            "pb_cross_pb2json",
            {{"host", "h1"}},
            {{"cpu", {55.5}}},
            {1000000000ULL});

        auto writeReq = makeProtoWriteRequest(writeBytes);
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Query via JSON request + JSON response
        auto queryReq = std::make_unique<seastar::http::request>();
        queryReq->_headers["Content-Type"] = "application/json";
        queryReq->content = R"({"query":"avg:pb_cross_pb2json(cpu){host:h1}","startTime":0,"endTime":2000000000})";

        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep));

        // Parse JSON response
        glz::generic parsed;
        auto ec = glz::read_json(parsed, queryRep->_content);
        ASSERT_FALSE(ec) << "Failed to parse query response as JSON";
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["status"].get<std::string>(), "success");

        auto& series = obj["series"].get<glz::generic::array_t>();
        EXPECT_GE(series.size(), 1u);
    })
        .join()
        .get();
}

TEST_F(ProtobufIntegrationTest, WriteProtobufWithJsonAcceptResponse) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        // Write via protobuf body but request JSON response
        auto writeBytes = buildProtoWriteRequest(
            "pb_json_accept",
            {{"zone", "b"}},
            {{"temp", {99.0}}},
            {1000000000ULL});

        auto req = makeProtoWriteJsonResponseRequest(writeBytes);
        auto rep = handler.handleWrite(std::move(req)).get();
        ASSERT_TRUE(isOk(*rep));

        // Response should be JSON (not protobuf)
        glz::generic parsed;
        auto ec = glz::read_json(parsed, rep->_content);
        ASSERT_FALSE(ec) << "Response should be valid JSON but got: " << rep->_content;
        auto& obj = parsed.get<glz::generic::object_t>();
        EXPECT_EQ(obj["status"].get<std::string>(), "success");
    })
        .join()
        .get();
}

TEST_F(ProtobufIntegrationTest, JsonBodyWithProtobufAcceptResponse) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write via JSON
        auto jsonReq = std::make_unique<seastar::http::request>();
        jsonReq->content = R"({
            "measurement": "pb_json_accept_resp",
            "tags": {"host": "h1"},
            "fields": {"value": 42.0},
            "timestamp": 1000000000
        })";
        jsonReq->_headers["Content-Type"] = "application/json";
        auto writeRep = writeHandler.handleWrite(std::move(jsonReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Query with JSON Content-Type but request protobuf response via Accept header
        auto queryJson = R"({"query":"avg:pb_json_accept_resp(value){host:h1}","startTime":0,"endTime":2000000000})";
        auto queryReq = makeJsonQueryProtoResponse(queryJson);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep));

        // Response should be protobuf
        auto queryResp = parseProtoQueryResponse(queryRep->_content);
        EXPECT_EQ(queryResp.status(), "success");
        EXPECT_GE(queryResp.series_size(), 1);
    })
        .join()
        .get();
}

// ============================================================================
// 4. Protobuf Query Error Tests
// ============================================================================

TEST_F(ProtobufIntegrationTest, QueryEmptyDatabaseProto) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpQueryHandler handler(&eng.eng);

        auto queryBytes = buildProtoQueryRequest("avg:nonexistent(value)", 0, 9999999999ULL);
        auto req = makeProtoQueryRequest(queryBytes);
        auto rep = handler.handleQuery(std::move(req)).get();
        ASSERT_TRUE(isOk(*rep));

        auto resp = parseProtoQueryResponse(rep->_content);
        EXPECT_EQ(resp.status(), "success");
        EXPECT_EQ(resp.series_size(), 0);
    })
        .join()
        .get();
}

TEST_F(ProtobufIntegrationTest, QueryInvalidTimeRangeProto) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpQueryHandler handler(&eng.eng);

        // startTime > endTime
        auto queryBytes = buildProtoQueryRequest("avg:metric(value)", 5000000000ULL, 1000000000ULL);
        auto req = makeProtoQueryRequest(queryBytes);
        auto rep = handler.handleQuery(std::move(req)).get();
        EXPECT_EQ(rep->_status, seastar::http::reply::status_type::bad_request);

        // Response should be parseable as protobuf error
        auto resp = parseProtoQueryResponse(rep->_content);
        EXPECT_FALSE(resp.error_message().empty());
    })
        .join()
        .get();
}

// ============================================================================
// 5. Protobuf Delete Tests
// ============================================================================

TEST_F(ProtobufIntegrationTest, WriteProtoThenDeleteProto) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpDeleteHandler deleteHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write data via protobuf
        auto writeBytes = buildProtoWriteRequest(
            "pb_delete_test",
            {{"host", "h1"}},
            {{"value", {10.0, 20.0, 30.0}}},
            {1000000000ULL, 2000000000ULL, 3000000000ULL});

        auto writeReq = makeProtoWriteRequest(writeBytes);
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Delete via protobuf
        auto deleteBytes = buildProtoDeleteRequest("pb_delete_test", {{"host", "h1"}});
        auto deleteReq = makeProtoDeleteRequest(deleteBytes);
        auto deleteRep = deleteHandler.handleDelete(std::move(deleteReq)).get();
        ASSERT_TRUE(isOk(*deleteRep));

        auto deleteResp = parseProtoDeleteResponse(deleteRep->_content);
        EXPECT_EQ(deleteResp.status(), "success");

        // Verify the query still works after deletion (tombstones applied).
        // Note: In-memory store with tombstones may still return data until
        // TSM rollover, so we only verify the query itself succeeds.
        auto queryBytes = buildProtoQueryRequest(
            "avg:pb_delete_test(value){host:h1}", 0, 4000000000ULL);
        auto queryReq = makeProtoQueryRequest(queryBytes);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep));

        auto queryResp = parseProtoQueryResponse(queryRep->_content);
        EXPECT_EQ(queryResp.status(), "success");
    })
        .join()
        .get();
}

// ============================================================================
// 6. Metadata Endpoints with Protobuf Accept
// ============================================================================

TEST_F(ProtobufIntegrationTest, MeasurementsWithProtobufAccept) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpMetadataHandler metaHandler(&eng.eng);

        // Write some data first
        auto writeBytes = buildProtoBatchWriteRequest({
            {"pb_meta_temp", {{"loc", "a"}}, {{"value", {72.5}}}, {1000000000ULL}},
            {"pb_meta_humid", {{"loc", "a"}}, {{"value", {55.0}}}, {1000000000ULL}},
        });

        auto writeReq = makeProtoWriteRequest(writeBytes);
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Query measurements with protobuf Accept
        auto metaReq = makeMetadataRequestProto();
        auto metaRep = metaHandler.handleMeasurements(std::move(metaReq)).get();
        ASSERT_TRUE(isOk(*metaRep));

        // Parse as protobuf MeasurementsResponse
        ::timestar_pb::MeasurementsResponse resp;
        ASSERT_TRUE(resp.ParseFromString(metaRep->_content));
        EXPECT_GE(resp.measurements_size(), 2);

        std::set<std::string> found;
        for (int i = 0; i < resp.measurements_size(); ++i) {
            found.insert(resp.measurements(i));
        }
        EXPECT_TRUE(found.count("pb_meta_temp") > 0);
        EXPECT_TRUE(found.count("pb_meta_humid") > 0);
    })
        .join()
        .get();
}

TEST_F(ProtobufIntegrationTest, TagsWithProtobufAccept) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpMetadataHandler metaHandler(&eng.eng);

        // Write data with various tags
        auto writeBytes = buildProtoBatchWriteRequest({
            {"pb_meta_tags_cpu", {{"host", "server-01"}, {"dc", "us-west"}}, {{"usage", {55.0}}}, {1000000000ULL}},
            {"pb_meta_tags_cpu", {{"host", "server-02"}, {"dc", "us-east"}}, {{"usage", {45.0}}}, {1000000000ULL}},
        });

        auto writeReq = makeProtoWriteRequest(writeBytes);
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Query tags with protobuf Accept
        auto metaReq = makeMetadataRequestProto("pb_meta_tags_cpu");
        auto metaRep = metaHandler.handleTags(std::move(metaReq)).get();
        ASSERT_TRUE(isOk(*metaRep));

        // Parse as protobuf TagsResponse
        ::timestar_pb::TagsResponse resp;
        ASSERT_TRUE(resp.ParseFromString(metaRep->_content));
        EXPECT_EQ(resp.measurement(), "pb_meta_tags_cpu");
        EXPECT_GE(resp.tags_size(), 2);  // host, dc
    })
        .join()
        .get();
}

TEST_F(ProtobufIntegrationTest, FieldsWithProtobufAccept) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpMetadataHandler metaHandler(&eng.eng);

        // Write data with multiple fields
        auto writeBytes = buildProtoWriteRequest(
            "pb_meta_fields_system",
            {{"host", "h1"}},
            {{"cpu_usage", {55.0}}, {"memory_usage", {12.5}}, {"disk_usage", {80.0}}},
            {1000000000ULL});

        auto writeReq = makeProtoWriteRequest(writeBytes);
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Query fields with protobuf Accept
        auto metaReq = makeMetadataRequestProto("pb_meta_fields_system");
        auto metaRep = metaHandler.handleFields(std::move(metaReq)).get();
        ASSERT_TRUE(isOk(*metaRep));

        // Parse as protobuf FieldsResponse
        ::timestar_pb::FieldsResponse resp;
        ASSERT_TRUE(resp.ParseFromString(metaRep->_content));
        EXPECT_EQ(resp.measurement(), "pb_meta_fields_system");
        EXPECT_GE(resp.fields_size(), 3);
    })
        .join()
        .get();
}

// ============================================================================
// 7. Group-by Queries via Protobuf
// ============================================================================

TEST_F(ProtobufIntegrationTest, QueryGroupByProto) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Write data for different hosts
        auto writeBytes = buildProtoBatchWriteRequest({
            {"pb_groupby_cpu", {{"host", "server-01"}, {"dc", "us-west"}}, {{"usage", {40.0}}}, {1000000000ULL}},
            {"pb_groupby_cpu", {{"host", "server-02"}, {"dc", "us-west"}}, {{"usage", {60.0}}}, {1000000000ULL}},
            {"pb_groupby_cpu", {{"host", "server-03"}, {"dc", "us-east"}}, {{"usage", {80.0}}}, {1000000000ULL}},
        });

        auto writeReq = makeProtoWriteRequest(writeBytes);
        auto writeRep = writeHandler.handleWrite(std::move(writeReq)).get();
        ASSERT_TRUE(isOk(*writeRep));

        // Query with group by dc via protobuf
        auto queryBytes = buildProtoQueryRequest("avg:pb_groupby_cpu(usage) by {dc}", 0, 2000000000ULL);
        auto queryReq = makeProtoQueryRequest(queryBytes);
        auto queryRep = queryHandler.handleQuery(std::move(queryReq)).get();
        ASSERT_TRUE(isOk(*queryRep));

        auto queryResp = parseProtoQueryResponse(queryRep->_content);
        EXPECT_EQ(queryResp.status(), "success");
        // Should have at least 2 series (one per dc group)
        EXPECT_GE(queryResp.series_size(), 2);
    })
        .join()
        .get();
}

// ============================================================================
// 8. Bool and String field types via Protobuf
// ============================================================================

TEST_F(ProtobufIntegrationTest, WriteBoolFieldProto) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        // Build a protobuf write with bool fields
        ::timestar_pb::WriteRequest pbReq;
        auto* wp = pbReq.add_writes();
        wp->set_measurement("pb_bool_test");
        (*wp->mutable_tags())["host"] = "h1";
        wp->add_timestamps(1000000000ULL);
        wp->add_timestamps(2000000000ULL);

        ::timestar_pb::WriteField wf;
        wf.mutable_bool_values()->add_values(true);
        wf.mutable_bool_values()->add_values(false);
        (*wp->mutable_fields())["healthy"] = wf;

        std::string bytes;
        pbReq.SerializeToString(&bytes);

        auto req = makeProtoWriteRequest(bytes);
        auto rep = handler.handleWrite(std::move(req)).get();
        ASSERT_TRUE(isOk(*rep));

        auto resp = parseProtoWriteResponse(rep->_content);
        EXPECT_EQ(resp.status(), "success");
        EXPECT_GE(resp.points_written(), 2);
    })
        .join()
        .get();
}

TEST_F(ProtobufIntegrationTest, WriteStringFieldProto) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        // Build a protobuf write with string fields
        ::timestar_pb::WriteRequest pbReq;
        auto* wp = pbReq.add_writes();
        wp->set_measurement("pb_string_test");
        (*wp->mutable_tags())["host"] = "h1";
        wp->add_timestamps(1000000000ULL);
        wp->add_timestamps(2000000000ULL);

        ::timestar_pb::WriteField wf;
        wf.mutable_string_values()->add_values("hello world");
        wf.mutable_string_values()->add_values("special chars: \t\n\"\\");
        (*wp->mutable_fields())["message"] = wf;

        std::string bytes;
        pbReq.SerializeToString(&bytes);

        auto req = makeProtoWriteRequest(bytes);
        auto rep = handler.handleWrite(std::move(req)).get();
        ASSERT_TRUE(isOk(*rep));

        auto resp = parseProtoWriteResponse(rep->_content);
        EXPECT_EQ(resp.status(), "success");
        EXPECT_GE(resp.points_written(), 2);
    })
        .join()
        .get();
}

TEST_F(ProtobufIntegrationTest, WriteIntegerFieldProto) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        // Build a protobuf write with int64 fields
        ::timestar_pb::WriteRequest pbReq;
        auto* wp = pbReq.add_writes();
        wp->set_measurement("pb_int_test");
        (*wp->mutable_tags())["host"] = "h1";
        wp->add_timestamps(1000000000ULL);
        wp->add_timestamps(2000000000ULL);

        ::timestar_pb::WriteField wf;
        wf.mutable_int64_values()->add_values(42);
        wf.mutable_int64_values()->add_values(-100);
        (*wp->mutable_fields())["counter"] = wf;

        std::string bytes;
        pbReq.SerializeToString(&bytes);

        auto req = makeProtoWriteRequest(bytes);
        auto rep = handler.handleWrite(std::move(req)).get();
        ASSERT_TRUE(isOk(*rep));

        auto resp = parseProtoWriteResponse(rep->_content);
        EXPECT_EQ(resp.status(), "success");
        EXPECT_GE(resp.points_written(), 2);
    })
        .join()
        .get();
}

// ============================================================================
// 9. Mixed field types in a single write via Protobuf
// ============================================================================

TEST_F(ProtobufIntegrationTest, WriteMixedTypesProto) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        // Build a protobuf write with multiple field types
        ::timestar_pb::WriteRequest pbReq;
        auto* wp = pbReq.add_writes();
        wp->set_measurement("pb_mixed_system");
        (*wp->mutable_tags())["host"] = "h1";
        wp->add_timestamps(1000000000ULL);

        // Double field
        ::timestar_pb::WriteField cpuField;
        cpuField.mutable_double_values()->add_values(55.5);
        (*wp->mutable_fields())["cpu_usage"] = cpuField;

        // Bool field
        ::timestar_pb::WriteField healthField;
        healthField.mutable_bool_values()->add_values(true);
        (*wp->mutable_fields())["is_healthy"] = healthField;

        // String field
        ::timestar_pb::WriteField statusField;
        statusField.mutable_string_values()->add_values("running");
        (*wp->mutable_fields())["status"] = statusField;

        // Integer field
        ::timestar_pb::WriteField counterField;
        counterField.mutable_int64_values()->add_values(1234);
        (*wp->mutable_fields())["counter"] = counterField;

        std::string bytes;
        pbReq.SerializeToString(&bytes);

        auto req = makeProtoWriteRequest(bytes);
        auto rep = handler.handleWrite(std::move(req)).get();
        ASSERT_TRUE(isOk(*rep));

        auto resp = parseProtoWriteResponse(rep->_content);
        EXPECT_EQ(resp.status(), "success");
        // 4 fields * 1 timestamp = 4 points
        EXPECT_GE(resp.points_written(), 4);
    })
        .join()
        .get();
}
