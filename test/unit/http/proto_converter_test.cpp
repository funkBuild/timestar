// Unit tests for proto_converters.hpp/cpp
//
// Tests use the intermediate types defined in proto_converters.hpp and the
// protobuf-generated types from timestar.pb.h.  Internal types (QueryResponse,
// StreamingBatch, etc.) are NOT used because they collide with proto names.

#include "proto_converters.hpp"
#include "timestar.pb.h"

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <string>
#include <vector>

using namespace timestar::proto;

// ============================================================================
// Write converter tests
// ============================================================================

TEST(ProtoConverterWrite, ParseEmptyWriteRequest) {
    ::timestar_pb::WriteRequest req;
    std::string bytes;
    req.SerializeToString(&bytes);

    auto points = parseWriteRequest(bytes.data(), bytes.size());
    EXPECT_TRUE(points.empty());
}

TEST(ProtoConverterWrite, ParseSingleDoubleField) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("cpu");
    (*wp->mutable_tags())["host"] = "server01";
    wp->add_timestamps(1000000000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(99.5);
    (*wp->mutable_fields())["usage"] = wf;

    std::string bytes;
    req.SerializeToString(&bytes);

    auto points = parseWriteRequest(bytes.data(), bytes.size());
    ASSERT_EQ(points.size(), 1u);
    EXPECT_EQ(points[0].measurement, "cpu");
    EXPECT_EQ(points[0].tags.at("host"), "server01");
    ASSERT_EQ(points[0].timestamps.size(), 1u);
    EXPECT_EQ(points[0].timestamps[0], 1000000000ULL);
    ASSERT_EQ(points[0].fields.count("usage"), 1u);
    EXPECT_EQ(points[0].fields.at("usage").type, FieldArrays::DOUBLE);
    ASSERT_EQ(points[0].fields.at("usage").doubles.size(), 1u);
    EXPECT_DOUBLE_EQ(points[0].fields.at("usage").doubles[0], 99.5);
}

TEST(ProtoConverterWrite, ParseBoolField) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("status");
    wp->add_timestamps(2000000000ULL);
    wp->add_timestamps(2000001000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_bool_values()->add_values(true);
    wf.mutable_bool_values()->add_values(false);
    (*wp->mutable_fields())["alive"] = wf;

    std::string bytes;
    req.SerializeToString(&bytes);

    auto points = parseWriteRequest(bytes.data(), bytes.size());
    ASSERT_EQ(points.size(), 1u);
    EXPECT_EQ(points[0].fields.at("alive").type, FieldArrays::BOOL);
    ASSERT_EQ(points[0].fields.at("alive").bools.size(), 2u);
    EXPECT_EQ(points[0].fields.at("alive").bools[0], 1);
    EXPECT_EQ(points[0].fields.at("alive").bools[1], 0);
}

TEST(ProtoConverterWrite, ParseStringField) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("logs");
    wp->add_timestamps(3000000000ULL);
    wp->add_timestamps(3000001000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_string_values()->add_values("hello world");
    wf.mutable_string_values()->add_values("special chars: \t\n\"\\");
    (*wp->mutable_fields())["message"] = wf;

    std::string bytes;
    req.SerializeToString(&bytes);

    auto points = parseWriteRequest(bytes.data(), bytes.size());
    ASSERT_EQ(points.size(), 1u);
    EXPECT_EQ(points[0].fields.at("message").type, FieldArrays::STRING);
    ASSERT_EQ(points[0].fields.at("message").strings.size(), 2u);
    EXPECT_EQ(points[0].fields.at("message").strings[0], "hello world");
    EXPECT_EQ(points[0].fields.at("message").strings[1], "special chars: \t\n\"\\");
}

TEST(ProtoConverterWrite, ParseIntegerField) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("counters");
    wp->add_timestamps(4000000000ULL);
    wp->add_timestamps(4000001000ULL);
    wp->add_timestamps(4000002000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_int64_values()->add_values(42);
    wf.mutable_int64_values()->add_values(-100);
    wf.mutable_int64_values()->add_values(INT64_MAX);
    (*wp->mutable_fields())["count"] = wf;

    std::string bytes;
    req.SerializeToString(&bytes);

    auto points = parseWriteRequest(bytes.data(), bytes.size());
    ASSERT_EQ(points.size(), 1u);
    EXPECT_EQ(points[0].fields.at("count").type, FieldArrays::INTEGER);
    ASSERT_EQ(points[0].fields.at("count").integers.size(), 3u);
    EXPECT_EQ(points[0].fields.at("count").integers[0], 42);
    EXPECT_EQ(points[0].fields.at("count").integers[1], -100);
    EXPECT_EQ(points[0].fields.at("count").integers[2], INT64_MAX);
}

TEST(ProtoConverterWrite, ParseMultipleWritePoints) {
    ::timestar_pb::WriteRequest req;

    auto* wp1 = req.add_writes();
    wp1->set_measurement("temp");
    wp1->add_timestamps(1000ULL);
    ::timestar_pb::WriteField wf1;
    wf1.mutable_double_values()->add_values(22.5);
    (*wp1->mutable_fields())["celsius"] = wf1;

    auto* wp2 = req.add_writes();
    wp2->set_measurement("humidity");
    wp2->add_timestamps(2000ULL);
    ::timestar_pb::WriteField wf2;
    wf2.mutable_double_values()->add_values(65.0);
    (*wp2->mutable_fields())["percent"] = wf2;

    std::string bytes;
    req.SerializeToString(&bytes);

    auto points = parseWriteRequest(bytes.data(), bytes.size());
    ASSERT_EQ(points.size(), 2u);
    EXPECT_EQ(points[0].measurement, "temp");
    EXPECT_EQ(points[1].measurement, "humidity");
}

TEST(ProtoConverterWrite, ParseInvalidBytes) {
    std::string garbage = "not a valid protobuf";
    EXPECT_NO_THROW({
        try {
            auto points = parseWriteRequest(garbage.data(), garbage.size());
        } catch (const std::runtime_error&) {
            // Expected if protobuf rejects it
        }
    });
}

TEST(ProtoConverterWrite, FormatSuccessResponse) {
    auto bytes = formatWriteResponse("success", 100);
    ::timestar_pb::WriteResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "success");
    EXPECT_EQ(resp.points_written(), 100);
    EXPECT_EQ(resp.failed_writes(), 0);
    EXPECT_EQ(resp.errors_size(), 0);
}

TEST(ProtoConverterWrite, FormatPartialFailureResponse) {
    auto bytes = formatWriteResponse("partial", 80, 20, {"error 1", "error 2"});
    ::timestar_pb::WriteResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "partial");
    EXPECT_EQ(resp.points_written(), 80);
    EXPECT_EQ(resp.failed_writes(), 20);
    ASSERT_EQ(resp.errors_size(), 2);
    EXPECT_EQ(resp.errors(0), "error 1");
    EXPECT_EQ(resp.errors(1), "error 2");
}

TEST(ProtoConverterWrite, FormatErrorResponse) {
    auto bytes = formatWriteResponse("error", 0, 5, {"bad data"});
    ::timestar_pb::WriteResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "error");
    EXPECT_EQ(resp.points_written(), 0);
    EXPECT_EQ(resp.failed_writes(), 5);
}

// ============================================================================
// Query converter tests
// ============================================================================

TEST(ProtoConverterQuery, ParseQueryRequest) {
    ::timestar_pb::QueryRequest req;
    req.set_query("avg:temperature(value){location:us-west} by {host}");
    req.set_start_time(1000000000ULL);
    req.set_end_time(2000000000ULL);
    req.set_aggregation_interval("5m");

    std::string bytes;
    req.SerializeToString(&bytes);

    auto parsed = parseQueryRequest(bytes.data(), bytes.size());
    EXPECT_EQ(parsed.query, "avg:temperature(value){location:us-west} by {host}");
    EXPECT_EQ(parsed.startTime, 1000000000ULL);
    EXPECT_EQ(parsed.endTime, 2000000000ULL);
    EXPECT_EQ(parsed.aggregationInterval, "5m");
}

TEST(ProtoConverterQuery, ParseQueryRequestMinimal) {
    ::timestar_pb::QueryRequest req;
    req.set_query("avg:cpu()");

    std::string bytes;
    req.SerializeToString(&bytes);

    auto parsed = parseQueryRequest(bytes.data(), bytes.size());
    EXPECT_EQ(parsed.query, "avg:cpu()");
    EXPECT_EQ(parsed.startTime, 0u);
    EXPECT_EQ(parsed.endTime, 0u);
    EXPECT_TRUE(parsed.aggregationInterval.empty());
}

TEST(ProtoConverterQuery, FormatQueryResponseDoubleValues) {
    QueryResponseData response;
    response.success = true;

    SeriesResultData sr;
    sr.measurement = "temperature";
    sr.tags["location"] = "us-west";

    std::vector<uint64_t> timestamps = {1000ULL, 2000ULL, 3000ULL};
    std::vector<double> values = {23.5, 24.1, 22.9};
    sr.fields["value"] = {timestamps, FieldValues{values}};
    response.series.push_back(std::move(sr));

    response.statistics.seriesCount = 1;
    response.statistics.pointCount = 3;
    response.statistics.executionTimeMs = 12.5;

    auto bytes = formatQueryResponse(response);
    ::timestar_pb::QueryResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "success");
    ASSERT_EQ(resp.series_size(), 1);

    const auto& series = resp.series(0);
    EXPECT_EQ(series.measurement(), "temperature");
    EXPECT_EQ(series.tags().at("location"), "us-west");

    ASSERT_EQ(series.fields().count("value"), 1u);
    const auto& fd = series.fields().at("value");
    ASSERT_EQ(fd.timestamps_size(), 3);
    EXPECT_EQ(fd.timestamps(0), 1000ULL);
    EXPECT_EQ(fd.timestamps(2), 3000ULL);

    ASSERT_TRUE(fd.has_double_values());
    ASSERT_EQ(fd.double_values().values_size(), 3);
    EXPECT_DOUBLE_EQ(fd.double_values().values(0), 23.5);
    EXPECT_DOUBLE_EQ(fd.double_values().values(1), 24.1);

    EXPECT_EQ(resp.statistics().series_count(), 1u);
    EXPECT_EQ(resp.statistics().point_count(), 3u);
    EXPECT_DOUBLE_EQ(resp.statistics().execution_time_ms(), 12.5);
}

TEST(ProtoConverterQuery, FormatQueryResponseBoolValues) {
    QueryResponseData response;
    response.success = true;

    SeriesResultData sr;
    sr.measurement = "status";

    std::vector<uint64_t> timestamps = {1000ULL, 2000ULL};
    std::vector<bool> values = {true, false};
    sr.fields["alive"] = {timestamps, FieldValues{values}};
    response.series.push_back(std::move(sr));
    response.statistics.seriesCount = 1;
    response.statistics.pointCount = 2;

    auto bytes = formatQueryResponse(response);
    ::timestar_pb::QueryResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));

    const auto& fd = resp.series(0).fields().at("alive");
    ASSERT_TRUE(fd.has_bool_values());
    ASSERT_EQ(fd.bool_values().values_size(), 2);
    EXPECT_TRUE(fd.bool_values().values(0));
    EXPECT_FALSE(fd.bool_values().values(1));
}

TEST(ProtoConverterQuery, FormatQueryResponseStringValues) {
    QueryResponseData response;
    response.success = true;

    SeriesResultData sr;
    sr.measurement = "logs";

    std::vector<uint64_t> timestamps = {5000ULL};
    std::vector<std::string> values = {"hello"};
    sr.fields["msg"] = {timestamps, FieldValues{values}};
    response.series.push_back(std::move(sr));
    response.statistics.seriesCount = 1;
    response.statistics.pointCount = 1;

    auto bytes = formatQueryResponse(response);
    ::timestar_pb::QueryResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));

    const auto& fd = resp.series(0).fields().at("msg");
    ASSERT_TRUE(fd.has_string_values());
    ASSERT_EQ(fd.string_values().values_size(), 1);
    EXPECT_EQ(fd.string_values().values(0), "hello");
}

TEST(ProtoConverterQuery, FormatQueryResponseInt64Values) {
    QueryResponseData response;
    response.success = true;

    SeriesResultData sr;
    sr.measurement = "counters";

    std::vector<uint64_t> timestamps = {6000ULL, 7000ULL};
    std::vector<int64_t> values = {42, -100};
    sr.fields["count"] = {timestamps, FieldValues{values}};
    response.series.push_back(std::move(sr));
    response.statistics.seriesCount = 1;
    response.statistics.pointCount = 2;

    auto bytes = formatQueryResponse(response);
    ::timestar_pb::QueryResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));

    const auto& fd = resp.series(0).fields().at("count");
    ASSERT_TRUE(fd.has_int64_values());
    ASSERT_EQ(fd.int64_values().values_size(), 2);
    EXPECT_EQ(fd.int64_values().values(0), 42);
    EXPECT_EQ(fd.int64_values().values(1), -100);
}

TEST(ProtoConverterQuery, FormatQueryResponseWithStatistics) {
    QueryResponseData response;
    response.success = true;
    response.statistics.seriesCount = 5;
    response.statistics.pointCount = 1000;
    response.statistics.executionTimeMs = 45.7;
    response.statistics.shardsQueried = {0, 1, 3};
    response.statistics.failedSeriesCount = 2;
    response.statistics.truncated = true;
    response.statistics.truncationReason = "max series limit";

    auto bytes = formatQueryResponse(response);
    ::timestar_pb::QueryResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));

    const auto& stats = resp.statistics();
    EXPECT_EQ(stats.series_count(), 5u);
    EXPECT_EQ(stats.point_count(), 1000u);
    EXPECT_DOUBLE_EQ(stats.execution_time_ms(), 45.7);
    ASSERT_EQ(stats.shards_queried_size(), 3);
    EXPECT_EQ(stats.shards_queried(0), 0);
    EXPECT_EQ(stats.shards_queried(1), 1);
    EXPECT_EQ(stats.shards_queried(2), 3);
    EXPECT_EQ(stats.failed_series_count(), 2u);
    EXPECT_TRUE(stats.truncated());
    EXPECT_EQ(stats.truncation_reason(), "max series limit");
}

TEST(ProtoConverterQuery, FormatQueryError) {
    auto bytes = formatQueryError("INVALID_QUERY", "Missing measurement");
    ::timestar_pb::QueryResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "error");
    EXPECT_EQ(resp.error_code(), "INVALID_QUERY");
    EXPECT_EQ(resp.error_message(), "Missing measurement");
}

TEST(ProtoConverterQuery, FormatQueryResponseEmpty) {
    QueryResponseData response;
    response.success = true;
    response.statistics.seriesCount = 0;
    response.statistics.pointCount = 0;

    auto bytes = formatQueryResponse(response);
    ::timestar_pb::QueryResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "success");
    EXPECT_EQ(resp.series_size(), 0);
}

TEST(ProtoConverterQuery, FormatQueryResponseNaNDoubles) {
    QueryResponseData response;
    response.success = true;

    SeriesResultData sr;
    sr.measurement = "test";

    std::vector<uint64_t> timestamps = {1000ULL, 2000ULL};
    std::vector<double> values = {std::numeric_limits<double>::quiet_NaN(), 42.0};
    sr.fields["value"] = {timestamps, FieldValues{values}};
    response.series.push_back(std::move(sr));
    response.statistics.pointCount = 2;
    response.statistics.seriesCount = 1;

    auto bytes = formatQueryResponse(response);
    ::timestar_pb::QueryResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));

    const auto& fd = resp.series(0).fields().at("value");
    ASSERT_EQ(fd.double_values().values_size(), 2);
    EXPECT_TRUE(std::isnan(fd.double_values().values(0)));
    EXPECT_DOUBLE_EQ(fd.double_values().values(1), 42.0);
}

// ============================================================================
// Delete converter tests
// ============================================================================

TEST(ProtoConverterDelete, ParseSingleDeleteBySeriesKey) {
    ::timestar_pb::DeleteRequest dr;
    dr.set_series("temperature,location=us-west.value");
    dr.set_start_time(1000ULL);
    dr.set_end_time(2000ULL);

    std::string bytes;
    dr.SerializeToString(&bytes);

    auto parsed = parseSingleDeleteRequest(bytes.data(), bytes.size());
    EXPECT_EQ(parsed.seriesKey, "temperature,location=us-west.value");
    EXPECT_FALSE(parsed.isStructured);
    EXPECT_EQ(parsed.startTime, 1000ULL);
    EXPECT_EQ(parsed.endTime, 2000ULL);
}

TEST(ProtoConverterDelete, ParseSingleDeleteStructured) {
    ::timestar_pb::DeleteRequest dr;
    dr.set_measurement("temperature");
    (*dr.mutable_tags())["location"] = "us-west";
    dr.set_field("value");

    std::string bytes;
    dr.SerializeToString(&bytes);

    auto parsed = parseSingleDeleteRequest(bytes.data(), bytes.size());
    EXPECT_EQ(parsed.measurement, "temperature");
    EXPECT_TRUE(parsed.isStructured);
    EXPECT_FALSE(parsed.isPattern);
    EXPECT_EQ(parsed.field, "value");
    EXPECT_EQ(parsed.tags.at("location"), "us-west");
    EXPECT_EQ(parsed.endTime, UINT64_MAX);  // default (end_time=0 -> UINT64_MAX)
}

TEST(ProtoConverterDelete, ParseSingleDeletePattern) {
    ::timestar_pb::DeleteRequest dr;
    dr.set_measurement("temperature");
    dr.add_fields("value");
    dr.add_fields("humidity");

    std::string bytes;
    dr.SerializeToString(&bytes);

    auto parsed = parseSingleDeleteRequest(bytes.data(), bytes.size());
    EXPECT_TRUE(parsed.isPattern);
    ASSERT_EQ(parsed.fields.size(), 2u);
    EXPECT_EQ(parsed.fields[0], "value");
    EXPECT_EQ(parsed.fields[1], "humidity");
}

TEST(ProtoConverterDelete, ParseBatchDeleteRequest) {
    ::timestar_pb::BatchDeleteRequest req;

    auto* d1 = req.add_deletes();
    d1->set_series("cpu.usage");
    d1->set_start_time(100ULL);
    d1->set_end_time(200ULL);

    auto* d2 = req.add_deletes();
    d2->set_measurement("memory");
    d2->set_field("used");

    std::string bytes;
    req.SerializeToString(&bytes);

    auto parsed = parseBatchDeleteRequest(bytes.data(), bytes.size());
    ASSERT_EQ(parsed.size(), 2u);
    EXPECT_EQ(parsed[0].seriesKey, "cpu.usage");
    EXPECT_FALSE(parsed[0].isStructured);
    EXPECT_EQ(parsed[1].measurement, "memory");
    EXPECT_TRUE(parsed[1].isStructured);
}

TEST(ProtoConverterDelete, FormatDeleteResponse) {
    auto bytes = formatDeleteResponse("success", 10, 10);
    ::timestar_pb::DeleteResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "success");
    EXPECT_EQ(resp.deleted_count(), 10u);
    EXPECT_EQ(resp.total_requests(), 10u);
    EXPECT_TRUE(resp.error_message().empty());
}

TEST(ProtoConverterDelete, FormatDeleteResponseWithError) {
    auto bytes = formatDeleteResponse("error", 0, 1, "series not found");
    ::timestar_pb::DeleteResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "error");
    EXPECT_EQ(resp.error_message(), "series not found");
}

// ============================================================================
// Metadata converter tests
// ============================================================================

TEST(ProtoConverterMetadata, FormatMeasurementsResponse) {
    auto bytes = formatMeasurementsResponse({"cpu", "memory", "disk"}, 5);
    ::timestar_pb::MeasurementsResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "success");
    ASSERT_EQ(resp.measurements_size(), 3);
    EXPECT_EQ(resp.measurements(0), "cpu");
    EXPECT_EQ(resp.measurements(1), "memory");
    EXPECT_EQ(resp.measurements(2), "disk");
    EXPECT_EQ(resp.total(), 5u);
}

TEST(ProtoConverterMetadata, FormatMeasurementsResponseEmpty) {
    auto bytes = formatMeasurementsResponse({}, 0);
    ::timestar_pb::MeasurementsResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.measurements_size(), 0);
    EXPECT_EQ(resp.total(), 0u);
}

TEST(ProtoConverterMetadata, FormatTagsResponse) {
    std::unordered_map<std::string, std::vector<std::string>> tags;
    tags["host"] = {"server01", "server02"};
    tags["region"] = {"us-west", "us-east", "eu-central"};

    auto bytes = formatTagsResponse("cpu", tags);
    ::timestar_pb::TagsResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "success");
    EXPECT_EQ(resp.measurement(), "cpu");

    ASSERT_EQ(resp.tags().count("host"), 1u);
    const auto& hostVals = resp.tags().at("host");
    ASSERT_EQ(hostVals.values_size(), 2);

    ASSERT_EQ(resp.tags().count("region"), 1u);
    const auto& regionVals = resp.tags().at("region");
    ASSERT_EQ(regionVals.values_size(), 3);
}

TEST(ProtoConverterMetadata, FormatFieldsResponse) {
    std::unordered_map<std::string, std::string> fields;
    fields["usage"] = "float";
    fields["active"] = "bool";
    fields["name"] = "string";

    auto bytes = formatFieldsResponse("cpu", fields);
    ::timestar_pb::FieldsResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "success");
    EXPECT_EQ(resp.measurement(), "cpu");
    ASSERT_EQ(resp.fields_size(), 3);

    std::map<std::string, std::string> parsedFields;
    for (int i = 0; i < resp.fields_size(); ++i) {
        parsedFields[resp.fields(i).name()] = resp.fields(i).type();
    }
    EXPECT_EQ(parsedFields["usage"], "float");
    EXPECT_EQ(parsedFields["active"], "bool");
    EXPECT_EQ(parsedFields["name"], "string");
}

TEST(ProtoConverterMetadata, FormatCardinalityResponse) {
    std::unordered_map<std::string, double> tagCard;
    tagCard["host"] = 100.0;
    tagCard["region"] = 5.0;

    auto bytes = formatCardinalityResponse("cpu", 500.0, tagCard);
    ::timestar_pb::CardinalityResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "success");
    EXPECT_EQ(resp.measurement(), "cpu");
    EXPECT_DOUBLE_EQ(resp.estimated_series_count(), 500.0);
    ASSERT_EQ(resp.tag_cardinalities_size(), 2);

    std::map<std::string, double> parsedCard;
    for (int i = 0; i < resp.tag_cardinalities_size(); ++i) {
        parsedCard[resp.tag_cardinalities(i).tag_key()] =
            resp.tag_cardinalities(i).estimated_count();
    }
    EXPECT_DOUBLE_EQ(parsedCard["host"], 100.0);
    EXPECT_DOUBLE_EQ(parsedCard["region"], 5.0);
}

// ============================================================================
// Retention converter tests
// ============================================================================

TEST(ProtoConverterRetention, ParseRetentionPutRequestBasic) {
    ::timestar_pb::RetentionPutRequest req;
    req.set_measurement("temperature");
    req.set_ttl("90d");

    std::string bytes;
    req.SerializeToString(&bytes);

    auto parsed = parseRetentionPutRequest(bytes.data(), bytes.size());
    EXPECT_EQ(parsed.measurement, "temperature");
    EXPECT_EQ(parsed.ttl, "90d");
    EXPECT_FALSE(parsed.downsample.has_value());
}

TEST(ProtoConverterRetention, ParseRetentionPutRequestWithDownsample) {
    ::timestar_pb::RetentionPutRequest req;
    req.set_measurement("cpu");
    req.set_ttl("30d");

    auto* ds = req.mutable_downsample();
    ds->set_after("7d");
    ds->set_after_nanos(604800000000000ULL);
    ds->set_interval("5m");
    ds->set_interval_nanos(300000000000ULL);
    ds->set_method("avg");

    std::string bytes;
    req.SerializeToString(&bytes);

    auto parsed = parseRetentionPutRequest(bytes.data(), bytes.size());
    EXPECT_EQ(parsed.measurement, "cpu");
    EXPECT_EQ(parsed.ttl, "30d");
    ASSERT_TRUE(parsed.downsample.has_value());
    EXPECT_EQ(parsed.downsample->after, "7d");
    EXPECT_EQ(parsed.downsample->afterNanos, 604800000000000ULL);
    EXPECT_EQ(parsed.downsample->interval, "5m");
    EXPECT_EQ(parsed.downsample->intervalNanos, 300000000000ULL);
    EXPECT_EQ(parsed.downsample->method, "avg");
}

TEST(ProtoConverterRetention, FormatRetentionGetResponse) {
    RetentionPolicyData policy;
    policy.measurement = "cpu";
    policy.ttl = "90d";
    policy.ttlNanos = 7776000000000000ULL;
    ParsedRetentionPutRequest::DownsampleData ds;
    ds.after = "30d";
    ds.afterNanos = 2592000000000000ULL;
    ds.interval = "1h";
    ds.intervalNanos = 3600000000000ULL;
    ds.method = "avg";
    policy.downsample = ds;

    auto bytes = formatRetentionGetResponse(policy);
    ::timestar_pb::RetentionGetResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "success");
    EXPECT_EQ(resp.policy().measurement(), "cpu");
    EXPECT_EQ(resp.policy().ttl(), "90d");
    EXPECT_EQ(resp.policy().ttl_nanos(), 7776000000000000ULL);
    EXPECT_TRUE(resp.policy().has_downsample());
    EXPECT_EQ(resp.policy().downsample().after(), "30d");
    EXPECT_EQ(resp.policy().downsample().method(), "avg");
}

TEST(ProtoConverterRetention, FormatRetentionGetResponseNoDownsample) {
    RetentionPolicyData policy;
    policy.measurement = "test";
    policy.ttl = "7d";
    policy.ttlNanos = 604800000000000ULL;

    auto bytes = formatRetentionGetResponse(policy);
    ::timestar_pb::RetentionGetResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.policy().measurement(), "test");
    EXPECT_FALSE(resp.policy().has_downsample());
}

TEST(ProtoConverterRetention, FormatStatusResponse) {
    auto bytes = formatStatusResponse("success", "Policy created", "");
    ::timestar_pb::StatusResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "success");
    EXPECT_EQ(resp.message(), "Policy created");
    EXPECT_TRUE(resp.code().empty());
}

TEST(ProtoConverterRetention, FormatStatusResponseError) {
    auto bytes = formatStatusResponse("error", "Invalid TTL", "INVALID_TTL");
    ::timestar_pb::StatusResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "error");
    EXPECT_EQ(resp.message(), "Invalid TTL");
    EXPECT_EQ(resp.code(), "INVALID_TTL");
}

// ============================================================================
// Streaming converter tests
// ============================================================================

TEST(ProtoConverterStreaming, ParseSubscribeRequestSingleQuery) {
    ::timestar_pb::SubscribeRequest req;
    req.set_query("avg:temperature(value){location:us-west}");
    req.set_backfill(true);
    req.set_start_time(1000000000ULL);
    req.set_aggregation_interval("5m");

    std::string bytes;
    req.SerializeToString(&bytes);

    auto parsed = parseSubscribeRequest(bytes.data(), bytes.size());
    EXPECT_EQ(parsed.query, "avg:temperature(value){location:us-west}");
    EXPECT_TRUE(parsed.backfill);
    EXPECT_EQ(parsed.startTime, 1000000000ULL);
    EXPECT_EQ(parsed.aggregationInterval, "5m");
    EXPECT_TRUE(parsed.queries.empty());
}

TEST(ProtoConverterStreaming, ParseSubscribeRequestMultiQuery) {
    ::timestar_pb::SubscribeRequest req;
    auto* q1 = req.add_queries();
    q1->set_query("avg:cpu(usage)");
    q1->set_label("a");
    auto* q2 = req.add_queries();
    q2->set_query("avg:memory(used)");
    q2->set_label("b");
    req.set_formula("a + b");

    std::string bytes;
    req.SerializeToString(&bytes);

    auto parsed = parseSubscribeRequest(bytes.data(), bytes.size());
    ASSERT_EQ(parsed.queries.size(), 2u);
    EXPECT_EQ(parsed.queries[0].query, "avg:cpu(usage)");
    EXPECT_EQ(parsed.queries[0].label, "a");
    EXPECT_EQ(parsed.queries[1].query, "avg:memory(used)");
    EXPECT_EQ(parsed.queries[1].label, "b");
    EXPECT_EQ(parsed.formula, "a + b");
}

TEST(ProtoConverterStreaming, FormatStreamingBatchDouble) {
    StreamingBatchData batch;
    batch.sequenceId = 42;
    batch.label = "test_label";
    batch.isDrop = false;
    batch.droppedCount = 0;

    StreamingDataPointData pt;
    pt.measurement = "cpu";
    pt.field = "usage";
    pt.tags = {{"host", "server01"}};
    pt.timestamp = 1000ULL;
    pt.value = 95.5;
    batch.points.push_back(pt);

    auto bytes = formatStreamingBatch(batch);
    ::timestar_pb::StreamingBatch pbBatch;
    ASSERT_TRUE(pbBatch.ParseFromString(bytes));
    EXPECT_EQ(pbBatch.sequence_id(), 42u);
    EXPECT_EQ(pbBatch.label(), "test_label");
    EXPECT_FALSE(pbBatch.is_drop());
    ASSERT_EQ(pbBatch.points_size(), 1);

    const auto& p = pbBatch.points(0);
    EXPECT_EQ(p.measurement(), "cpu");
    EXPECT_EQ(p.field(), "usage");
    EXPECT_EQ(p.timestamp(), 1000ULL);
    EXPECT_EQ(p.tags().at("host"), "server01");
    EXPECT_TRUE(p.value().has_double_value());
    EXPECT_DOUBLE_EQ(p.value().double_value(), 95.5);
}

TEST(ProtoConverterStreaming, FormatStreamingBatchBool) {
    StreamingBatchData batch;
    batch.sequenceId = 1;

    StreamingDataPointData pt;
    pt.measurement = "status";
    pt.field = "alive";
    pt.timestamp = 2000ULL;
    pt.value = true;
    batch.points.push_back(pt);

    auto bytes = formatStreamingBatch(batch);
    ::timestar_pb::StreamingBatch pbBatch;
    ASSERT_TRUE(pbBatch.ParseFromString(bytes));
    EXPECT_TRUE(pbBatch.points(0).value().has_bool_value());
    EXPECT_TRUE(pbBatch.points(0).value().bool_value());
}

TEST(ProtoConverterStreaming, FormatStreamingBatchString) {
    StreamingBatchData batch;
    batch.sequenceId = 1;

    StreamingDataPointData pt;
    pt.measurement = "logs";
    pt.field = "msg";
    pt.timestamp = 3000ULL;
    pt.value = std::string("hello world");
    batch.points.push_back(pt);

    auto bytes = formatStreamingBatch(batch);
    ::timestar_pb::StreamingBatch pbBatch;
    ASSERT_TRUE(pbBatch.ParseFromString(bytes));
    EXPECT_TRUE(pbBatch.points(0).value().has_string_value());
    EXPECT_EQ(pbBatch.points(0).value().string_value(), "hello world");
}

TEST(ProtoConverterStreaming, FormatStreamingBatchInt64) {
    StreamingBatchData batch;
    batch.sequenceId = 1;

    StreamingDataPointData pt;
    pt.measurement = "counters";
    pt.field = "count";
    pt.timestamp = 4000ULL;
    pt.value = int64_t(-42);
    batch.points.push_back(pt);

    auto bytes = formatStreamingBatch(batch);
    ::timestar_pb::StreamingBatch pbBatch;
    ASSERT_TRUE(pbBatch.ParseFromString(bytes));
    EXPECT_TRUE(pbBatch.points(0).value().has_int64_value());
    EXPECT_EQ(pbBatch.points(0).value().int64_value(), -42);
}

TEST(ProtoConverterStreaming, FormatStreamingBatchDrop) {
    StreamingBatchData batch;
    batch.sequenceId = 10;
    batch.isDrop = true;
    batch.droppedCount = 500;

    auto bytes = formatStreamingBatch(batch);
    ::timestar_pb::StreamingBatch pbBatch;
    ASSERT_TRUE(pbBatch.ParseFromString(bytes));
    EXPECT_TRUE(pbBatch.is_drop());
    EXPECT_EQ(pbBatch.dropped_count(), 500u);
    EXPECT_EQ(pbBatch.points_size(), 0);
}

TEST(ProtoConverterStreaming, FormatSubscriptionsResponse) {
    std::vector<SubscriptionStatsData> subs;

    SubscriptionStatsData s1;
    s1.id = 1;
    s1.measurement = "cpu";
    s1.scopes = {{"host", "server01"}};
    s1.fields = {"usage", "idle"};
    s1.label = "cpu_sub";
    s1.handlerShard = 0;
    s1.queueDepth = 10;
    s1.queueCapacity = 1000;
    s1.droppedPoints = 5;
    s1.eventsSent = 100;
    subs.push_back(s1);

    auto bytes = formatSubscriptionsResponse(subs);
    ::timestar_pb::SubscriptionsResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "success");
    ASSERT_EQ(resp.subscriptions_size(), 1);

    const auto& sub = resp.subscriptions(0);
    EXPECT_EQ(sub.id(), 1u);
    EXPECT_EQ(sub.measurement(), "cpu");
    EXPECT_EQ(sub.scopes().at("host"), "server01");
    ASSERT_EQ(sub.fields_size(), 2);
    EXPECT_EQ(sub.fields(0), "usage");
    EXPECT_EQ(sub.fields(1), "idle");
    EXPECT_EQ(sub.label(), "cpu_sub");
    EXPECT_EQ(sub.handler_shard(), 0u);
    EXPECT_EQ(sub.queue_depth(), 10u);
    EXPECT_EQ(sub.queue_capacity(), 1000u);
    EXPECT_EQ(sub.dropped_points(), 5u);
    EXPECT_EQ(sub.events_sent(), 100u);
}

// ============================================================================
// Derived query converter tests
// ============================================================================

TEST(ProtoConverterDerived, ParseDerivedQueryRequest) {
    ::timestar_pb::DerivedQueryRequest req;

    auto* q1 = req.add_queries();
    q1->set_name("a");
    q1->set_query("avg:cpu(usage)");

    auto* q2 = req.add_queries();
    q2->set_name("b");
    q2->set_query("avg:memory(used)");

    req.set_formula("(a + b) / 2");
    req.set_start_time(1000000000ULL);
    req.set_end_time(2000000000ULL);
    req.set_aggregation_interval("5m");

    std::string bytes;
    req.SerializeToString(&bytes);

    auto parsed = parseDerivedQueryRequest(bytes.data(), bytes.size());
    ASSERT_EQ(parsed.queries.size(), 2u);
    EXPECT_EQ(parsed.queries.at("a"), "avg:cpu(usage)");
    EXPECT_EQ(parsed.queries.at("b"), "avg:memory(used)");
    EXPECT_EQ(parsed.formula, "(a + b) / 2");
    EXPECT_EQ(parsed.startTime, 1000000000ULL);
    EXPECT_EQ(parsed.endTime, 2000000000ULL);
    EXPECT_EQ(parsed.aggregationInterval, "5m");
}

TEST(ProtoConverterDerived, ParseDerivedQueryRequestMinimal) {
    ::timestar_pb::DerivedQueryRequest req;
    auto* q1 = req.add_queries();
    q1->set_name("x");
    q1->set_query("sum:disk(free)");
    req.set_formula("x");

    std::string bytes;
    req.SerializeToString(&bytes);

    auto parsed = parseDerivedQueryRequest(bytes.data(), bytes.size());
    ASSERT_EQ(parsed.queries.size(), 1u);
    EXPECT_EQ(parsed.queries.at("x"), "sum:disk(free)");
    EXPECT_EQ(parsed.formula, "x");
    EXPECT_EQ(parsed.startTime, 0u);
    EXPECT_EQ(parsed.endTime, 0u);
    EXPECT_TRUE(parsed.aggregationInterval.empty());
}

TEST(ProtoConverterDerived, FormatDerivedQueryResponse) {
    DerivedQueryResultData result;
    result.timestamps = {1000ULL, 2000ULL, 3000ULL};
    result.values = {10.0, 20.0, 30.0};
    result.formula = "(a + b) / 2";
    result.stats.pointCount = 3;
    result.stats.executionTimeMs = 5.5;
    result.stats.subQueriesExecuted = 2;
    result.stats.pointsDroppedDueToAlignment = 1;

    auto bytes = formatDerivedQueryResponse(result);
    ::timestar_pb::DerivedQueryResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "success");
    EXPECT_EQ(resp.formula(), "(a + b) / 2");
    ASSERT_EQ(resp.timestamps_size(), 3);
    EXPECT_EQ(resp.timestamps(0), 1000ULL);
    ASSERT_EQ(resp.values_size(), 3);
    EXPECT_DOUBLE_EQ(resp.values(0), 10.0);

    const auto& stats = resp.statistics();
    EXPECT_EQ(stats.point_count(), 3u);
    EXPECT_DOUBLE_EQ(stats.execution_time_ms(), 5.5);
    EXPECT_EQ(stats.sub_queries_executed(), 2u);
    EXPECT_EQ(stats.points_dropped_due_to_alignment(), 1u);
}

TEST(ProtoConverterDerived, FormatDerivedQueryError) {
    auto bytes = formatDerivedQueryError("QUERY_ERROR", "undefined variable 'z'");
    ::timestar_pb::DerivedQueryResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "error");
    EXPECT_EQ(resp.error_code(), "QUERY_ERROR");
    EXPECT_EQ(resp.error_message(), "undefined variable 'z'");
}

// ============================================================================
// Anomaly response converter tests
// ============================================================================

TEST(ProtoConverterAnomaly, FormatAnomalyResponseSuccess) {
    AnomalyQueryResultData result;
    result.success = true;
    result.times = {1000ULL, 2000ULL, 3000ULL};

    AnomalySeriesPieceData raw;
    raw.piece = "raw";
    raw.groupTags = {"host=server01"};
    raw.values = {10.0, 20.0, 30.0};
    result.series.push_back(raw);

    AnomalySeriesPieceData scores;
    scores.piece = "scores";
    scores.values = {0.1, 0.9, 0.3};
    scores.alertValue = 0.9;
    result.series.push_back(scores);

    result.statistics.algorithm = "basic";
    result.statistics.bounds = 2.0;
    result.statistics.seasonality = "none";
    result.statistics.anomalyCount = 1;
    result.statistics.totalPoints = 3;
    result.statistics.executionTimeMs = 8.2;

    auto bytes = formatAnomalyResponse(result);
    ::timestar_pb::AnomalyResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "success");
    ASSERT_EQ(resp.times_size(), 3);
    ASSERT_EQ(resp.series_size(), 2);

    const auto& rawPiece = resp.series(0);
    EXPECT_EQ(rawPiece.piece(), "raw");
    ASSERT_EQ(rawPiece.group_tags_size(), 1);
    EXPECT_EQ(rawPiece.group_tags(0), "host=server01");
    ASSERT_EQ(rawPiece.values_size(), 3);
    EXPECT_FALSE(rawPiece.has_alert());

    const auto& scoresPiece = resp.series(1);
    EXPECT_EQ(scoresPiece.piece(), "scores");
    EXPECT_TRUE(scoresPiece.has_alert());
    EXPECT_DOUBLE_EQ(scoresPiece.alert_value(), 0.9);

    const auto& stats = resp.statistics();
    EXPECT_EQ(stats.algorithm(), "basic");
    EXPECT_DOUBLE_EQ(stats.bounds(), 2.0);
    EXPECT_EQ(stats.anomaly_count(), 1u);
}

TEST(ProtoConverterAnomaly, FormatAnomalyResponseError) {
    AnomalyQueryResultData result;
    result.success = false;
    result.errorMessage = "insufficient data";

    auto bytes = formatAnomalyResponse(result);
    ::timestar_pb::AnomalyResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "error");
    EXPECT_EQ(resp.error_message(), "insufficient data");
}

// ============================================================================
// Forecast response converter tests
// ============================================================================

TEST(ProtoConverterForecast, FormatForecastResponse) {
    ForecastQueryResultData result;
    result.success = true;
    result.times = {1000ULL, 2000ULL, 3000ULL, 4000ULL};
    result.forecastStartIndex = 2;

    ForecastSeriesPieceData past;
    past.piece = "past";
    past.values = {10.0, 20.0, std::nullopt, std::nullopt};
    result.series.push_back(past);

    ForecastSeriesPieceData forecast;
    forecast.piece = "forecast";
    forecast.values = {std::nullopt, std::nullopt, 30.0, 40.0};
    result.series.push_back(forecast);

    result.statistics.algorithm = "linear";
    result.statistics.deviations = 2.0;
    result.statistics.slope = 10.0;
    result.statistics.intercept = 0.0;
    result.statistics.rSquared = 0.99;
    result.statistics.residualStdDev = 1.5;
    result.statistics.historicalPoints = 2;
    result.statistics.forecastPoints = 2;
    result.statistics.seriesCount = 1;
    result.statistics.executionTimeMs = 3.1;

    auto bytes = formatForecastResponse(result);
    ::timestar_pb::ForecastResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "success");
    ASSERT_EQ(resp.times_size(), 4);
    EXPECT_EQ(resp.forecast_start_index(), 2u);
    ASSERT_EQ(resp.series_size(), 2);

    const auto& pastPiece = resp.series(0);
    EXPECT_EQ(pastPiece.piece(), "past");
    ASSERT_EQ(pastPiece.values_size(), 4);
    EXPECT_DOUBLE_EQ(pastPiece.values(0), 10.0);
    EXPECT_DOUBLE_EQ(pastPiece.values(1), 20.0);
    EXPECT_TRUE(std::isnan(pastPiece.values(2)));
    EXPECT_TRUE(std::isnan(pastPiece.values(3)));

    const auto& forecastPiece = resp.series(1);
    EXPECT_EQ(forecastPiece.piece(), "forecast");
    EXPECT_TRUE(std::isnan(forecastPiece.values(0)));
    EXPECT_TRUE(std::isnan(forecastPiece.values(1)));
    EXPECT_DOUBLE_EQ(forecastPiece.values(2), 30.0);
    EXPECT_DOUBLE_EQ(forecastPiece.values(3), 40.0);

    const auto& stats = resp.statistics();
    EXPECT_EQ(stats.algorithm(), "linear");
    EXPECT_DOUBLE_EQ(stats.deviations(), 2.0);
    EXPECT_DOUBLE_EQ(stats.slope(), 10.0);
    EXPECT_DOUBLE_EQ(stats.r_squared(), 0.99);
    EXPECT_EQ(stats.historical_points(), 2u);
    EXPECT_EQ(stats.forecast_points(), 2u);
}

TEST(ProtoConverterForecast, FormatForecastResponseError) {
    ForecastQueryResultData result;
    result.success = false;
    result.errorMessage = "not enough data";

    auto bytes = formatForecastResponse(result);
    ::timestar_pb::ForecastResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "error");
    EXPECT_EQ(resp.error_message(), "not enough data");
}

// ============================================================================
// Health / error converter tests
// ============================================================================

TEST(ProtoConverterHealth, FormatHealthResponse) {
    auto bytes = formatHealthResponse("ok");
    ::timestar_pb::HealthResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "ok");
}

TEST(ProtoConverterHealth, FormatErrorResponse) {
    auto bytes = formatErrorResponse("something went wrong", "INTERNAL_ERROR");
    ::timestar_pb::StatusResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "error");
    EXPECT_EQ(resp.message(), "something went wrong");
    EXPECT_EQ(resp.code(), "INTERNAL_ERROR");
}

TEST(ProtoConverterHealth, FormatErrorResponseNoCode) {
    auto bytes = formatErrorResponse("bad request");
    ::timestar_pb::StatusResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "error");
    EXPECT_EQ(resp.message(), "bad request");
    EXPECT_TRUE(resp.code().empty());
}

// ============================================================================
// Round-trip tests
// ============================================================================

TEST(ProtoConverterRoundTrip, WriteRequestRoundTrip) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("temperature");
    (*wp->mutable_tags())["location"] = "us-west";
    (*wp->mutable_tags())["sensor"] = "temp-01";
    wp->add_timestamps(1000000000ULL);
    wp->add_timestamps(1000001000ULL);

    ::timestar_pb::WriteField doubleField;
    doubleField.mutable_double_values()->add_values(22.5);
    doubleField.mutable_double_values()->add_values(23.1);
    (*wp->mutable_fields())["celsius"] = doubleField;

    ::timestar_pb::WriteField boolField;
    boolField.mutable_bool_values()->add_values(true);
    boolField.mutable_bool_values()->add_values(false);
    (*wp->mutable_fields())["active"] = boolField;

    std::string bytes;
    req.SerializeToString(&bytes);

    auto points = parseWriteRequest(bytes.data(), bytes.size());
    ASSERT_EQ(points.size(), 1u);

    const auto& mwp = points[0];
    EXPECT_EQ(mwp.measurement, "temperature");
    EXPECT_EQ(mwp.tags.size(), 2u);
    EXPECT_EQ(mwp.tags.at("location"), "us-west");
    EXPECT_EQ(mwp.tags.at("sensor"), "temp-01");
    EXPECT_EQ(mwp.timestamps.size(), 2u);

    ASSERT_EQ(mwp.fields.count("celsius"), 1u);
    EXPECT_EQ(mwp.fields.at("celsius").type, FieldArrays::DOUBLE);
    EXPECT_EQ(mwp.fields.at("celsius").doubles.size(), 2u);

    ASSERT_EQ(mwp.fields.count("active"), 1u);
    EXPECT_EQ(mwp.fields.at("active").type, FieldArrays::BOOL);
    EXPECT_EQ(mwp.fields.at("active").bools.size(), 2u);
}

TEST(ProtoConverterRoundTrip, QueryResponseRoundTrip) {
    QueryResponseData response;
    response.success = true;

    SeriesResultData sr;
    sr.measurement = "cpu";
    sr.tags["host"] = "server01";

    std::vector<uint64_t> ts = {100ULL, 200ULL, 300ULL};
    std::vector<double> vals = {10.0, 20.0, 30.0};
    sr.fields["usage"] = {ts, FieldValues{vals}};

    std::vector<uint64_t> ts2 = {100ULL, 200ULL};
    std::vector<int64_t> ivals = {1, 2};
    sr.fields["count"] = {ts2, FieldValues{ivals}};

    response.series.push_back(std::move(sr));
    response.statistics.seriesCount = 1;
    response.statistics.pointCount = 5;
    response.statistics.executionTimeMs = 1.5;

    auto bytes = formatQueryResponse(response);

    ::timestar_pb::QueryResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "success");
    ASSERT_EQ(resp.series_size(), 1);

    const auto& series = resp.series(0);
    EXPECT_EQ(series.measurement(), "cpu");
    EXPECT_EQ(series.tags().at("host"), "server01");

    ASSERT_EQ(series.fields().count("usage"), 1u);
    const auto& usageFd = series.fields().at("usage");
    EXPECT_EQ(usageFd.timestamps_size(), 3);
    EXPECT_TRUE(usageFd.has_double_values());
    EXPECT_EQ(usageFd.double_values().values_size(), 3);

    ASSERT_EQ(series.fields().count("count"), 1u);
    const auto& countFd = series.fields().at("count");
    EXPECT_EQ(countFd.timestamps_size(), 2);
    EXPECT_TRUE(countFd.has_int64_values());
    EXPECT_EQ(countFd.int64_values().values_size(), 2);
}

TEST(ProtoConverterRoundTrip, LargeWriteBatch) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("metrics");

    ::timestar_pb::WriteField wf;
    auto* doubles = wf.mutable_double_values();
    for (int i = 0; i < 10000; ++i) {
        wp->add_timestamps(static_cast<uint64_t>(i) * 1000000ULL);
        doubles->add_values(static_cast<double>(i) * 0.1);
    }
    (*wp->mutable_fields())["value"] = wf;

    std::string bytes;
    req.SerializeToString(&bytes);

    auto points = parseWriteRequest(bytes.data(), bytes.size());
    ASSERT_EQ(points.size(), 1u);
    EXPECT_EQ(points[0].timestamps.size(), 10000u);
    EXPECT_EQ(points[0].fields.at("value").doubles.size(), 10000u);
    EXPECT_DOUBLE_EQ(points[0].fields.at("value").doubles[9999], 999.9);
}

TEST(ProtoConverterRoundTrip, ZeroTimestamp) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("test");
    wp->add_timestamps(0ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(1.0);
    (*wp->mutable_fields())["v"] = wf;

    std::string bytes;
    req.SerializeToString(&bytes);

    auto points = parseWriteRequest(bytes.data(), bytes.size());
    ASSERT_EQ(points.size(), 1u);
    ASSERT_EQ(points[0].timestamps.size(), 1u);
    EXPECT_EQ(points[0].timestamps[0], 0ULL);
}

TEST(ProtoConverterRoundTrip, EmptyTagsAndFields) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("bare");
    wp->add_timestamps(1000ULL);

    std::string bytes;
    req.SerializeToString(&bytes);

    auto points = parseWriteRequest(bytes.data(), bytes.size());
    ASSERT_EQ(points.size(), 1u);
    EXPECT_TRUE(points[0].tags.empty());
    EXPECT_TRUE(points[0].fields.empty());
}

TEST(ProtoConverterRoundTrip, MultipleFieldTypes) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("mixed");
    wp->add_timestamps(1000ULL);

    ::timestar_pb::WriteField df;
    df.mutable_double_values()->add_values(42.0);
    (*wp->mutable_fields())["dbl"] = df;

    ::timestar_pb::WriteField bf;
    bf.mutable_bool_values()->add_values(true);
    (*wp->mutable_fields())["boo"] = bf;

    ::timestar_pb::WriteField sf;
    sf.mutable_string_values()->add_values("test");
    (*wp->mutable_fields())["str"] = sf;

    ::timestar_pb::WriteField intf;
    intf.mutable_int64_values()->add_values(99);
    (*wp->mutable_fields())["num"] = intf;

    std::string bytes;
    req.SerializeToString(&bytes);

    auto points = parseWriteRequest(bytes.data(), bytes.size());
    ASSERT_EQ(points.size(), 1u);
    EXPECT_EQ(points[0].fields.size(), 4u);
    EXPECT_EQ(points[0].fields.at("dbl").type, FieldArrays::DOUBLE);
    EXPECT_EQ(points[0].fields.at("boo").type, FieldArrays::BOOL);
    EXPECT_EQ(points[0].fields.at("str").type, FieldArrays::STRING);
    EXPECT_EQ(points[0].fields.at("num").type, FieldArrays::INTEGER);
}

TEST(ProtoConverterRoundTrip, DerivedQueryRoundTrip) {
    // Build proto request
    ::timestar_pb::DerivedQueryRequest req;
    auto* q = req.add_queries();
    q->set_name("a");
    q->set_query("avg:cpu(usage)");
    req.set_formula("a * 100");
    req.set_start_time(1000ULL);
    req.set_end_time(2000ULL);
    req.set_aggregation_interval("1m");

    std::string reqBytes;
    req.SerializeToString(&reqBytes);

    // Parse
    auto parsed = parseDerivedQueryRequest(reqBytes.data(), reqBytes.size());
    EXPECT_EQ(parsed.queries.at("a"), "avg:cpu(usage)");
    EXPECT_EQ(parsed.formula, "a * 100");

    // Build response
    DerivedQueryResultData result;
    result.timestamps = {1000ULL, 1060000000000ULL};
    result.values = {95.0, 87.3};
    result.formula = "a * 100";
    result.stats.pointCount = 2;
    result.stats.executionTimeMs = 1.2;
    result.stats.subQueriesExecuted = 1;

    auto respBytes = formatDerivedQueryResponse(result);
    ::timestar_pb::DerivedQueryResponse resp;
    ASSERT_TRUE(resp.ParseFromString(respBytes));
    EXPECT_EQ(resp.status(), "success");
    EXPECT_EQ(resp.formula(), "a * 100");
    ASSERT_EQ(resp.timestamps_size(), 2);
    ASSERT_EQ(resp.values_size(), 2);
    EXPECT_DOUBLE_EQ(resp.values(0), 95.0);
}
