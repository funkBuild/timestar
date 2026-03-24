// Unit tests for proto_write_fast_path.hpp/cpp
//
// Tests the Arena-parsed, zero-intermediate-copy protobuf write fast path.
// This test links against libtimestar_proto_conv and timestar_proto (NOT
// libtimestar) to maintain ODR isolation.

#include "proto_write_fast_path.hpp"
#include "proto_converters.hpp"
#include "timestar.pb.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

using namespace timestar::proto;

// Helper: serialize a WriteRequest to bytes
static std::string serialize(const ::timestar_pb::WriteRequest& req) {
    std::string bytes;
    req.SerializeToString(&bytes);
    return bytes;
}

// Default timestamp for tests (2024-01-01 00:00:00 UTC in nanoseconds)
static constexpr uint64_t DEFAULT_TS = 1704067200000000000ULL;

// ============================================================================
// Basic field type tests
// ============================================================================

TEST(ProtoWriteFastPath, EmptyWriteRequest) {
    ::timestar_pb::WriteRequest req;
    auto bytes = serialize(req);

    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);
    EXPECT_TRUE(result.inserts.empty());
    EXPECT_EQ(result.totalPoints, 0);
    EXPECT_EQ(result.failedWrites, 0);
    EXPECT_TRUE(result.errors.empty());
}

TEST(ProtoWriteFastPath, SingleDoubleField) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("cpu");
    (*wp->mutable_tags())["host"] = "server01";
    wp->add_timestamps(1000000000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(99.5);
    (*wp->mutable_fields())["usage"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(result.inserts.size(), 1u);
    EXPECT_EQ(result.failedWrites, 0);
    EXPECT_EQ(result.totalPoints, 1);

    const auto& ins = result.inserts[0];
    EXPECT_EQ(ins.measurement, "cpu");
    EXPECT_EQ(ins.fieldName, "usage");
    EXPECT_EQ(ins.type, FastFieldInsert::Type::DOUBLE);
    ASSERT_EQ(ins.tags.size(), 1u);
    EXPECT_EQ(ins.tags.at("host"), "server01");
    ASSERT_EQ(ins.timestamps.size(), 1u);
    EXPECT_EQ(ins.timestamps[0], 1000000000ULL);
    ASSERT_EQ(ins.doubleValues.size(), 1u);
    EXPECT_DOUBLE_EQ(ins.doubleValues[0], 99.5);
    EXPECT_EQ(ins.seriesKey, "cpu,host=server01 usage");
}

TEST(ProtoWriteFastPath, BoolField) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("status");
    wp->add_timestamps(2000000000ULL);
    wp->add_timestamps(2000001000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_bool_values()->add_values(true);
    wf.mutable_bool_values()->add_values(false);
    (*wp->mutable_fields())["alive"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(result.inserts.size(), 1u);
    const auto& ins = result.inserts[0];
    EXPECT_EQ(ins.type, FastFieldInsert::Type::BOOL);
    ASSERT_EQ(ins.boolValues.size(), 2u);
    EXPECT_EQ(ins.boolValues[0], 1);
    EXPECT_EQ(ins.boolValues[1], 0);
    EXPECT_EQ(result.totalPoints, 2);
}

TEST(ProtoWriteFastPath, StringField) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("logs");
    wp->add_timestamps(3000000000ULL);
    wp->add_timestamps(3000001000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_string_values()->add_values("hello world");
    wf.mutable_string_values()->add_values("special chars: \t\n\"\\");
    (*wp->mutable_fields())["message"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(result.inserts.size(), 1u);
    const auto& ins = result.inserts[0];
    EXPECT_EQ(ins.type, FastFieldInsert::Type::STRING);
    ASSERT_EQ(ins.stringValues.size(), 2u);
    EXPECT_EQ(ins.stringValues[0], "hello world");
    EXPECT_EQ(ins.stringValues[1], "special chars: \t\n\"\\");
}

TEST(ProtoWriteFastPath, IntegerField) {
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

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(result.inserts.size(), 1u);
    const auto& ins = result.inserts[0];
    EXPECT_EQ(ins.type, FastFieldInsert::Type::INTEGER);
    ASSERT_EQ(ins.integerValues.size(), 3u);
    EXPECT_EQ(ins.integerValues[0], 42);
    EXPECT_EQ(ins.integerValues[1], -100);
    EXPECT_EQ(ins.integerValues[2], INT64_MAX);
    EXPECT_EQ(result.totalPoints, 3);
}

// ============================================================================
// Multi-field and multi-point tests
// ============================================================================

TEST(ProtoWriteFastPath, MultipleFieldsPerPoint) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("weather");
    (*wp->mutable_tags())["location"] = "us-west";
    wp->add_timestamps(5000000000ULL);

    ::timestar_pb::WriteField wfTemp;
    wfTemp.mutable_double_values()->add_values(23.5);
    (*wp->mutable_fields())["temperature"] = wfTemp;

    ::timestar_pb::WriteField wfHumidity;
    wfHumidity.mutable_double_values()->add_values(65.0);
    (*wp->mutable_fields())["humidity"] = wfHumidity;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(result.inserts.size(), 2u);
    EXPECT_EQ(result.totalPoints, 2);

    // Both inserts should have the same measurement and tags
    for (const auto& ins : result.inserts) {
        EXPECT_EQ(ins.measurement, "weather");
        EXPECT_EQ(ins.tags.at("location"), "us-west");
        ASSERT_EQ(ins.timestamps.size(), 1u);
        EXPECT_EQ(ins.timestamps[0], 5000000000ULL);
    }
}

TEST(ProtoWriteFastPath, MultipleWritePoints) {
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

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(result.inserts.size(), 2u);
    EXPECT_EQ(result.totalPoints, 2);
    EXPECT_EQ(result.inserts[0].measurement, "temp");
    EXPECT_EQ(result.inserts[1].measurement, "humidity");
}

TEST(ProtoWriteFastPath, MixedFieldTypes) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("mixed");
    wp->add_timestamps(6000000000ULL);

    ::timestar_pb::WriteField wfDouble;
    wfDouble.mutable_double_values()->add_values(42.0);
    (*wp->mutable_fields())["temperature"] = wfDouble;

    ::timestar_pb::WriteField wfBool;
    wfBool.mutable_bool_values()->add_values(true);
    (*wp->mutable_fields())["active"] = wfBool;

    ::timestar_pb::WriteField wfStr;
    wfStr.mutable_string_values()->add_values("running");
    (*wp->mutable_fields())["status"] = wfStr;

    ::timestar_pb::WriteField wfInt;
    wfInt.mutable_int64_values()->add_values(999);
    (*wp->mutable_fields())["count"] = wfInt;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(result.inserts.size(), 4u);
    EXPECT_EQ(result.totalPoints, 4);
    EXPECT_EQ(result.failedWrites, 0);

    // Verify each type is correct
    bool foundDouble = false, foundBool = false, foundString = false, foundInteger = false;
    for (const auto& ins : result.inserts) {
        if (ins.fieldName == "temperature") {
            EXPECT_EQ(ins.type, FastFieldInsert::Type::DOUBLE);
            foundDouble = true;
        } else if (ins.fieldName == "active") {
            EXPECT_EQ(ins.type, FastFieldInsert::Type::BOOL);
            foundBool = true;
        } else if (ins.fieldName == "status") {
            EXPECT_EQ(ins.type, FastFieldInsert::Type::STRING);
            foundString = true;
        } else if (ins.fieldName == "count") {
            EXPECT_EQ(ins.type, FastFieldInsert::Type::INTEGER);
            foundInteger = true;
        }
    }
    EXPECT_TRUE(foundDouble);
    EXPECT_TRUE(foundBool);
    EXPECT_TRUE(foundString);
    EXPECT_TRUE(foundInteger);
}

// ============================================================================
// Timestamp handling tests
// ============================================================================

TEST(ProtoWriteFastPath, NoTimestampsUsesDefault) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("notimestamp");

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(1.0);
    (*wp->mutable_fields())["value"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(result.inserts.size(), 1u);
    ASSERT_EQ(result.inserts[0].timestamps.size(), 1u);
    EXPECT_EQ(result.inserts[0].timestamps[0], DEFAULT_TS);
}

TEST(ProtoWriteFastPath, NoTimestampsMultipleValuesGeneratesSequence) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("notimestamp_multi");

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(1.0);
    wf.mutable_double_values()->add_values(2.0);
    wf.mutable_double_values()->add_values(3.0);
    (*wp->mutable_fields())["value"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(result.inserts.size(), 1u);
    ASSERT_EQ(result.inserts[0].timestamps.size(), 3u);
    EXPECT_EQ(result.inserts[0].timestamps[0], DEFAULT_TS);
    EXPECT_EQ(result.inserts[0].timestamps[1], DEFAULT_TS + 1000000ULL);
    EXPECT_EQ(result.inserts[0].timestamps[2], DEFAULT_TS + 2000000ULL);
}

TEST(ProtoWriteFastPath, SingleTimestampReplicatedForArray) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("replicated_ts");
    wp->add_timestamps(9999ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(1.0);
    wf.mutable_double_values()->add_values(2.0);
    wf.mutable_double_values()->add_values(3.0);
    (*wp->mutable_fields())["value"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(result.inserts.size(), 1u);
    ASSERT_EQ(result.inserts[0].timestamps.size(), 3u);
    for (uint64_t ts : result.inserts[0].timestamps) {
        EXPECT_EQ(ts, 9999ULL);
    }
}

TEST(ProtoWriteFastPath, PackedTimestampsExtracted) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("packed_ts");

    // Add 100 timestamps
    for (uint64_t i = 0; i < 100; ++i) {
        wp->add_timestamps(1000000000ULL + i * 1000000ULL);
    }

    ::timestar_pb::WriteField wf;
    for (int i = 0; i < 100; ++i) {
        wf.mutable_double_values()->add_values(static_cast<double>(i));
    }
    (*wp->mutable_fields())["value"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(result.inserts.size(), 1u);
    ASSERT_EQ(result.inserts[0].timestamps.size(), 100u);
    for (uint64_t i = 0; i < 100; ++i) {
        EXPECT_EQ(result.inserts[0].timestamps[i], 1000000000ULL + i * 1000000ULL);
    }
}

// ============================================================================
// Series key tests
// ============================================================================

TEST(ProtoWriteFastPath, SeriesKeyNoTags) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("cpu");
    wp->add_timestamps(1000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(50.0);
    (*wp->mutable_fields())["usage"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(result.inserts.size(), 1u);
    EXPECT_EQ(result.inserts[0].seriesKey, "cpu usage");
}

TEST(ProtoWriteFastPath, SeriesKeyMultipleTags) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("cpu");
    // Tags are stored in std::map (sorted), so "dc" < "host"
    (*wp->mutable_tags())["host"] = "server01";
    (*wp->mutable_tags())["dc"] = "us-east";
    wp->add_timestamps(1000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(50.0);
    (*wp->mutable_fields())["usage"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(result.inserts.size(), 1u);
    // std::map sorts keys: dc < host
    EXPECT_EQ(result.inserts[0].seriesKey, "cpu,dc=us-east,host=server01 usage");
}

// ============================================================================
// Validation tests
// ============================================================================

TEST(ProtoWriteFastPath, InvalidMeasurementName) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("cpu,bad");  // comma not allowed
    wp->add_timestamps(1000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(50.0);
    (*wp->mutable_fields())["usage"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    EXPECT_TRUE(result.inserts.empty());
    EXPECT_EQ(result.failedWrites, 1);
    EXPECT_FALSE(result.errors.empty());
}

TEST(ProtoWriteFastPath, EmptyMeasurementName) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("");
    wp->add_timestamps(1000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(50.0);
    (*wp->mutable_fields())["usage"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    EXPECT_TRUE(result.inserts.empty());
    EXPECT_EQ(result.failedWrites, 1);
}

TEST(ProtoWriteFastPath, InvalidTagKey) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("cpu");
    (*wp->mutable_tags())["bad=key"] = "value";
    wp->add_timestamps(1000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(50.0);
    (*wp->mutable_fields())["usage"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    EXPECT_TRUE(result.inserts.empty());
    EXPECT_EQ(result.failedWrites, 1);
}

TEST(ProtoWriteFastPath, InvalidFieldName) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("cpu");
    wp->add_timestamps(1000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(50.0);
    (*wp->mutable_fields())["bad field"] = wf;  // space not allowed in field name

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    // Invalid field is skipped, but the write point itself is not failed
    // unless all fields fail
    EXPECT_TRUE(result.inserts.empty());
    EXPECT_FALSE(result.errors.empty());
}

TEST(ProtoWriteFastPath, InvalidProtobufBytes) {
    std::string garbage = "not a valid protobuf message bytes";
    // Proto parser may accept garbage (protobuf is lenient), but should not crash.
    // If it truly rejects, it throws runtime_error.
    try {
        auto result = parseWriteRequestFast(garbage.data(), garbage.size(), DEFAULT_TS);
        // If it doesn't throw, result should be safe to use
        // (might have 0 or more inserts depending on proto leniency)
    } catch (const std::runtime_error&) {
        // Expected — invalid protobuf
    }
}

// ============================================================================
// Edge cases
// ============================================================================

TEST(ProtoWriteFastPath, EmptyFieldValues) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("cpu");
    wp->add_timestamps(1000ULL);

    // Field with no values (empty DoubleArray)
    ::timestar_pb::WriteField wf;
    wf.mutable_double_values();  // empty
    (*wp->mutable_fields())["usage"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    // Empty values should be skipped
    EXPECT_TRUE(result.inserts.empty());
}

TEST(ProtoWriteFastPath, UnsetOneofField) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("cpu");
    wp->add_timestamps(1000ULL);

    // Field with unset typed_values
    ::timestar_pb::WriteField wf;
    (*wp->mutable_fields())["usage"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    // Unset oneof should be skipped
    EXPECT_TRUE(result.inserts.empty());
}

TEST(ProtoWriteFastPath, TimestampCountMismatch) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("cpu");
    wp->add_timestamps(1000ULL);
    wp->add_timestamps(2000ULL);

    // 3 values but only 2 timestamps (and not 1, so it won't replicate)
    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(1.0);
    wf.mutable_double_values()->add_values(2.0);
    wf.mutable_double_values()->add_values(3.0);
    (*wp->mutable_fields())["value"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    // Timestamp mismatch should produce an error for that field
    EXPECT_TRUE(result.inserts.empty());
    EXPECT_FALSE(result.errors.empty());
}

// ============================================================================
// Large batch test
// ============================================================================

TEST(ProtoWriteFastPath, LargeBatch10000Points) {
    ::timestar_pb::WriteRequest req;

    // 10000 write points, each with 1 field and 1 timestamp
    for (int i = 0; i < 10000; ++i) {
        auto* wp = req.add_writes();
        wp->set_measurement("cpu");
        (*wp->mutable_tags())["host"] = "server" + std::to_string(i % 100);
        wp->add_timestamps(static_cast<uint64_t>(i) * 1000000000ULL);

        ::timestar_pb::WriteField wf;
        wf.mutable_double_values()->add_values(static_cast<double>(i) * 0.1);
        (*wp->mutable_fields())["usage"] = wf;
    }

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    EXPECT_EQ(result.inserts.size(), 10000u);
    EXPECT_EQ(result.totalPoints, 10000);
    EXPECT_EQ(result.failedWrites, 0);
    EXPECT_TRUE(result.errors.empty());
}

TEST(ProtoWriteFastPath, LargeBatchArrayWrite) {
    // Single write point with 10000 values in one field (columnar batch)
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("bulk");
    (*wp->mutable_tags())["source"] = "test";

    for (uint64_t i = 0; i < 10000; ++i) {
        wp->add_timestamps(1000000000ULL + i * 1000000ULL);
    }

    ::timestar_pb::WriteField wf;
    for (int i = 0; i < 10000; ++i) {
        wf.mutable_double_values()->add_values(static_cast<double>(i));
    }
    (*wp->mutable_fields())["value"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(result.inserts.size(), 1u);
    EXPECT_EQ(result.totalPoints, 10000);
    ASSERT_EQ(result.inserts[0].doubleValues.size(), 10000u);
    ASSERT_EQ(result.inserts[0].timestamps.size(), 10000u);

    // Verify data integrity after memcpy
    for (int i = 0; i < 10000; ++i) {
        EXPECT_DOUBLE_EQ(result.inserts[0].doubleValues[i], static_cast<double>(i));
        EXPECT_EQ(result.inserts[0].timestamps[i], 1000000000ULL + static_cast<uint64_t>(i) * 1000000ULL);
    }
}

// ============================================================================
// Comparison with generic path: identical results
// ============================================================================

TEST(ProtoWriteFastPath, MatchesGenericPathDoubles) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("weather");
    (*wp->mutable_tags())["location"] = "us-west";
    (*wp->mutable_tags())["sensor"] = "temp-01";
    wp->add_timestamps(1000000000ULL);
    wp->add_timestamps(1000001000ULL);
    wp->add_timestamps(1000002000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(23.5);
    wf.mutable_double_values()->add_values(24.0);
    wf.mutable_double_values()->add_values(23.8);
    (*wp->mutable_fields())["temperature"] = wf;

    auto bytes = serialize(req);

    // Parse with generic path
    auto genericPoints = parseWriteRequest(bytes.data(), bytes.size());

    // Parse with fast path
    auto fastResult = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    // Compare: generic path returns MultiWritePoints, fast path returns FastFieldInserts
    ASSERT_EQ(genericPoints.size(), 1u);
    ASSERT_EQ(fastResult.inserts.size(), 1u);

    const auto& gp = genericPoints[0];
    const auto& fi = fastResult.inserts[0];

    EXPECT_EQ(gp.measurement, fi.measurement);
    EXPECT_EQ(gp.tags, fi.tags);
    EXPECT_EQ(gp.timestamps, fi.timestamps);

    ASSERT_EQ(gp.fields.count("temperature"), 1u);
    const auto& gfa = gp.fields.at("temperature");
    EXPECT_EQ(gfa.type, FieldArrays::DOUBLE);
    EXPECT_EQ(fi.type, FastFieldInsert::Type::DOUBLE);

    ASSERT_EQ(gfa.doubles.size(), fi.doubleValues.size());
    for (size_t i = 0; i < gfa.doubles.size(); ++i) {
        EXPECT_DOUBLE_EQ(gfa.doubles[i], fi.doubleValues[i]);
    }
}

TEST(ProtoWriteFastPath, MatchesGenericPathIntegers) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("counters");
    wp->add_timestamps(5000000000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_int64_values()->add_values(42);
    wf.mutable_int64_values()->add_values(-1);
    (*wp->mutable_fields())["count"] = wf;

    // Add matching timestamps
    wp->add_timestamps(5000001000ULL);

    auto bytes = serialize(req);

    auto genericPoints = parseWriteRequest(bytes.data(), bytes.size());
    auto fastResult = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(genericPoints.size(), 1u);
    ASSERT_EQ(fastResult.inserts.size(), 1u);

    const auto& gfa = genericPoints[0].fields.at("count");
    const auto& fi = fastResult.inserts[0];

    EXPECT_EQ(gfa.type, FieldArrays::INTEGER);
    EXPECT_EQ(fi.type, FastFieldInsert::Type::INTEGER);
    ASSERT_EQ(gfa.integers.size(), fi.integerValues.size());
    for (size_t i = 0; i < gfa.integers.size(); ++i) {
        EXPECT_EQ(gfa.integers[i], fi.integerValues[i]);
    }
}

TEST(ProtoWriteFastPath, MatchesGenericPathBools) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("flags");
    wp->add_timestamps(6000000000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_bool_values()->add_values(true);
    (*wp->mutable_fields())["enabled"] = wf;

    auto bytes = serialize(req);

    auto genericPoints = parseWriteRequest(bytes.data(), bytes.size());
    auto fastResult = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(genericPoints.size(), 1u);
    ASSERT_EQ(fastResult.inserts.size(), 1u);

    const auto& gfa = genericPoints[0].fields.at("enabled");
    const auto& fi = fastResult.inserts[0];

    EXPECT_EQ(gfa.type, FieldArrays::BOOL);
    EXPECT_EQ(fi.type, FastFieldInsert::Type::BOOL);
    ASSERT_EQ(gfa.bools.size(), fi.boolValues.size());
    for (size_t i = 0; i < gfa.bools.size(); ++i) {
        EXPECT_EQ(gfa.bools[i], fi.boolValues[i]);
    }
}

TEST(ProtoWriteFastPath, MatchesGenericPathStrings) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("events");
    wp->add_timestamps(7000000000ULL);
    wp->add_timestamps(7000001000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_string_values()->add_values("event1");
    wf.mutable_string_values()->add_values("event2");
    (*wp->mutable_fields())["name"] = wf;

    auto bytes = serialize(req);

    auto genericPoints = parseWriteRequest(bytes.data(), bytes.size());
    auto fastResult = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(genericPoints.size(), 1u);
    ASSERT_EQ(fastResult.inserts.size(), 1u);

    const auto& gfa = genericPoints[0].fields.at("name");
    const auto& fi = fastResult.inserts[0];

    EXPECT_EQ(gfa.type, FieldArrays::STRING);
    EXPECT_EQ(fi.type, FastFieldInsert::Type::STRING);
    ASSERT_EQ(gfa.strings.size(), fi.stringValues.size());
    for (size_t i = 0; i < gfa.strings.size(); ++i) {
        EXPECT_EQ(gfa.strings[i], fi.stringValues[i]);
    }
}

// ============================================================================
// Tags with spaces (valid for tag values)
// ============================================================================

TEST(ProtoWriteFastPath, TagValueWithSpaces) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("cpu");
    (*wp->mutable_tags())["host"] = "server with spaces";
    wp->add_timestamps(1000ULL);

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(50.0);
    (*wp->mutable_fields())["usage"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(result.inserts.size(), 1u);
    EXPECT_EQ(result.inserts[0].tags.at("host"), "server with spaces");
    EXPECT_EQ(result.failedWrites, 0);
}

// ============================================================================
// Multiple fields share tags correctly
// ============================================================================

TEST(ProtoWriteFastPath, MultipleFieldsShareTags) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("system");
    (*wp->mutable_tags())["host"] = "srv01";
    (*wp->mutable_tags())["region"] = "us-east";
    wp->add_timestamps(8000000000ULL);
    wp->add_timestamps(8000001000ULL);

    ::timestar_pb::WriteField wf1;
    wf1.mutable_double_values()->add_values(75.0);
    wf1.mutable_double_values()->add_values(80.0);
    (*wp->mutable_fields())["cpu"] = wf1;

    ::timestar_pb::WriteField wf2;
    wf2.mutable_int64_values()->add_values(4096);
    wf2.mutable_int64_values()->add_values(4000);
    (*wp->mutable_fields())["memory"] = wf2;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), DEFAULT_TS);

    ASSERT_EQ(result.inserts.size(), 2u);
    for (const auto& ins : result.inserts) {
        EXPECT_EQ(ins.tags.size(), 2u);
        EXPECT_EQ(ins.tags.at("host"), "srv01");
        EXPECT_EQ(ins.tags.at("region"), "us-east");
        EXPECT_EQ(ins.measurement, "system");
    }
}
