// Gap-coverage tests for lib/http/proto_converters.cpp and
// lib/http/proto_write_fast_path.cpp.
//
// The existing proto_converter_test.cpp / proto_write_fast_path_test.cpp
// suites cover the uncompressed paths thoroughly.  This file targets the
// branches they miss:
//   - "Approach B" compressed write-request decode paths in parseWriteRequest
//     (compressed_timestamps / compressed_alp / compressed_rle /
//     compressed_zstd / compressed_ffor), exercised with REAL encoder output
//   - formatQueryResponse error branch (success == false)
//   - parse-failure throws for the non-write parse functions
//   - unset-oneof field skip in the generic parseWriteRequest
//   - fast path: invalid tag VALUE, error-list cap at 10, truncated-byte
//     robustness (must error cleanly or parse partially — never crash)

#include "proto_converters.hpp"
#include "proto_write_fast_path.hpp"
#include "timestar.pb.h"

// Real encoders — the same ones parseWriteRequest uses to decode
#include "bool_encoder_rle.hpp"
#include "float_encoder.hpp"
#include "integer_encoder.hpp"
#include "string_encoder.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

using namespace timestar::proto;

namespace {

std::string serialize(const ::timestar_pb::WriteRequest& req) {
    std::string bytes;
    req.SerializeToString(&bytes);
    return bytes;
}

// Build a WritePoint skeleton with measurement, one tag, and plain timestamps.
::timestar_pb::WritePoint* addPoint(::timestar_pb::WriteRequest& req, const std::vector<uint64_t>& timestamps) {
    auto* wp = req.add_writes();
    wp->set_measurement("gap_metric");
    (*wp->mutable_tags())["host"] = "server-01";
    for (uint64_t ts : timestamps) {
        wp->add_timestamps(ts);
    }
    return wp;
}

std::vector<uint64_t> makeTimestamps(size_t n) {
    std::vector<uint64_t> ts;
    ts.reserve(n);
    uint64_t base = 1704067200000000000ULL;  // 2024-01-01
    for (size_t i = 0; i < n; ++i) {
        ts.push_back(base + i * 1000000000ULL);
    }
    return ts;
}

}  // namespace

// ============================================================================
// Compressed write-request decode paths (Approach B)
// ============================================================================

TEST(ProtoCompressedWriteGap, CompressedTimestampsDecodedByGenericParser) {
    auto ts = makeTimestamps(200);

    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("gap_metric");
    (*wp->mutable_tags())["host"] = "server-01";

    // FFOR-compress the timestamps instead of using the repeated field
    auto encoded = IntegerEncoder::encode(std::span<const uint64_t>(ts));
    wp->set_compressed_timestamps(encoded.data.data(), encoded.size());

    // Plain double field so the point has values
    ::timestar_pb::WriteField wf;
    for (size_t i = 0; i < ts.size(); ++i) {
        wf.mutable_double_values()->add_values(static_cast<double>(i) * 0.5);
    }
    (*wp->mutable_fields())["value"] = wf;

    auto bytes = serialize(req);
    auto points = parseWriteRequest(bytes.data(), bytes.size());

    ASSERT_EQ(points.size(), 1u);
    ASSERT_EQ(points[0].timestamps.size(), ts.size());
    EXPECT_EQ(points[0].timestamps, ts);
    ASSERT_EQ(points[0].fields.at("value").doubles.size(), ts.size());
}

TEST(ProtoCompressedWriteGap, CompressedAlpDoublesRoundTrip) {
    auto ts = makeTimestamps(150);
    std::vector<double> values;
    values.reserve(ts.size());
    for (size_t i = 0; i < ts.size(); ++i) {
        values.push_back(20.0 + static_cast<double>(i) * 0.25);
    }

    ::timestar_pb::WriteRequest req;
    auto* wp = addPoint(req, ts);

    ::timestar_pb::WriteField wf;
    auto encoded = FloatEncoder::encode(std::span<const double>(values));
    wf.mutable_double_values()->set_compressed_alp(reinterpret_cast<const char*>(encoded.data.data()),
                                                   encoded.dataByteSize());
    (*wp->mutable_fields())["temp"] = wf;

    auto bytes = serialize(req);
    auto points = parseWriteRequest(bytes.data(), bytes.size());

    ASSERT_EQ(points.size(), 1u);
    const auto& fa = points[0].fields.at("temp");
    EXPECT_EQ(fa.type, timestar::FieldArrays::DOUBLE);
    ASSERT_EQ(fa.doubles.size(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_DOUBLE_EQ(fa.doubles[i], values[i]) << "index " << i;
    }
}

TEST(ProtoCompressedWriteGap, CompressedRleBoolsRoundTrip) {
    auto ts = makeTimestamps(64);
    std::vector<bool> values;
    values.reserve(ts.size());
    for (size_t i = 0; i < ts.size(); ++i) {
        values.push_back(i % 3 == 0);
    }

    ::timestar_pb::WriteRequest req;
    auto* wp = addPoint(req, ts);

    ::timestar_pb::WriteField wf;
    auto encoded = BoolEncoderRLE::encode(values);
    wf.mutable_bool_values()->set_compressed_rle(reinterpret_cast<const char*>(encoded.data.data()), encoded.size());
    (*wp->mutable_fields())["flag"] = wf;

    auto bytes = serialize(req);
    auto points = parseWriteRequest(bytes.data(), bytes.size());

    ASSERT_EQ(points.size(), 1u);
    const auto& fa = points[0].fields.at("flag");
    EXPECT_EQ(fa.type, timestar::FieldArrays::BOOL);
    ASSERT_EQ(fa.bools.size(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(fa.bools[i] != 0, values[i]) << "index " << i;
    }
}

TEST(ProtoCompressedWriteGap, CompressedZstdStringsRoundTripWithCount) {
    auto ts = makeTimestamps(40);
    std::vector<std::string> values;
    values.reserve(ts.size());
    for (size_t i = 0; i < ts.size(); ++i) {
        values.push_back("status-" + std::to_string(i % 5));
    }

    ::timestar_pb::WriteRequest req;
    auto* wp = addPoint(req, ts);

    ::timestar_pb::WriteField wf;
    auto encoded = StringEncoder::encode(std::span<const std::string>(values));
    wf.mutable_string_values()->set_compressed_zstd(reinterpret_cast<const char*>(encoded.data.data()),
                                                    encoded.size());
    wf.mutable_string_values()->set_count(static_cast<uint32_t>(values.size()));
    (*wp->mutable_fields())["status"] = wf;

    auto bytes = serialize(req);
    auto points = parseWriteRequest(bytes.data(), bytes.size());

    ASSERT_EQ(points.size(), 1u);
    const auto& fa = points[0].fields.at("status");
    EXPECT_EQ(fa.type, timestar::FieldArrays::STRING);
    ASSERT_EQ(fa.strings.size(), values.size());
    EXPECT_EQ(fa.strings, values);
}

TEST(ProtoCompressedWriteGap, CompressedZstdStringsCountFallsBackToTimestampCount) {
    // count == 0 -> the parser must fall back to timestamps.size()
    auto ts = makeTimestamps(12);
    std::vector<std::string> values;
    for (size_t i = 0; i < ts.size(); ++i) {
        values.push_back("v" + std::to_string(i));
    }

    ::timestar_pb::WriteRequest req;
    auto* wp = addPoint(req, ts);

    ::timestar_pb::WriteField wf;
    auto encoded = StringEncoder::encode(std::span<const std::string>(values));
    wf.mutable_string_values()->set_compressed_zstd(reinterpret_cast<const char*>(encoded.data.data()),
                                                    encoded.size());
    // deliberately do NOT set count
    (*wp->mutable_fields())["status"] = wf;

    auto bytes = serialize(req);
    auto points = parseWriteRequest(bytes.data(), bytes.size());

    ASSERT_EQ(points.size(), 1u);
    const auto& fa = points[0].fields.at("status");
    ASSERT_EQ(fa.strings.size(), values.size());
    EXPECT_EQ(fa.strings, values);
}

TEST(ProtoCompressedWriteGap, CompressedFforInt64ZigZagRoundTrip) {
    auto ts = makeTimestamps(100);
    std::vector<int64_t> values;
    values.reserve(ts.size());
    for (size_t i = 0; i < ts.size(); ++i) {
        // Mix of negative and positive values to exercise zigzag
        values.push_back((i % 2 == 0 ? -1 : 1) * static_cast<int64_t>(i * 1000));
    }

    // ZigZag encode then FFOR — mirrors formatQueryResponse's int64 encoding,
    // which is the documented wire contract for compressed_ffor
    std::vector<uint64_t> zz(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        zz[i] = (static_cast<uint64_t>(values[i]) << 1) ^ static_cast<uint64_t>(values[i] >> 63);
    }
    auto encoded = IntegerEncoder::encode(std::span<const uint64_t>(zz));

    ::timestar_pb::WriteRequest req;
    auto* wp = addPoint(req, ts);
    ::timestar_pb::WriteField wf;
    wf.mutable_int64_values()->set_compressed_ffor(reinterpret_cast<const char*>(encoded.data.data()),
                                                   encoded.size());
    (*wp->mutable_fields())["counter"] = wf;

    auto bytes = serialize(req);
    auto points = parseWriteRequest(bytes.data(), bytes.size());

    ASSERT_EQ(points.size(), 1u);
    const auto& fa = points[0].fields.at("counter");
    EXPECT_EQ(fa.type, timestar::FieldArrays::INTEGER);
    ASSERT_EQ(fa.integers.size(), values.size());
    EXPECT_EQ(fa.integers, values);
}

TEST(ProtoCompressedWriteGap, AllFieldsCompressedInOnePoint) {
    auto ts = makeTimestamps(80);
    std::vector<double> dv;
    std::vector<bool> bv;
    std::vector<std::string> sv;
    std::vector<int64_t> iv;
    for (size_t i = 0; i < ts.size(); ++i) {
        dv.push_back(static_cast<double>(i) * 1.5);
        bv.push_back(i % 2 == 0);
        sv.push_back("s" + std::to_string(i % 7));
        iv.push_back(static_cast<int64_t>(i) - 40);
    }

    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("gap_metric");
    (*wp->mutable_tags())["host"] = "server-01";

    auto tsEnc = IntegerEncoder::encode(std::span<const uint64_t>(ts));
    wp->set_compressed_timestamps(tsEnc.data.data(), tsEnc.size());

    {
        ::timestar_pb::WriteField wf;
        auto enc = FloatEncoder::encode(std::span<const double>(dv));
        wf.mutable_double_values()->set_compressed_alp(reinterpret_cast<const char*>(enc.data.data()),
                                                       enc.dataByteSize());
        (*wp->mutable_fields())["d"] = wf;
    }
    {
        ::timestar_pb::WriteField wf;
        auto enc = BoolEncoderRLE::encode(bv);
        wf.mutable_bool_values()->set_compressed_rle(reinterpret_cast<const char*>(enc.data.data()), enc.size());
        (*wp->mutable_fields())["b"] = wf;
    }
    {
        ::timestar_pb::WriteField wf;
        auto enc = StringEncoder::encode(std::span<const std::string>(sv));
        wf.mutable_string_values()->set_compressed_zstd(reinterpret_cast<const char*>(enc.data.data()), enc.size());
        wf.mutable_string_values()->set_count(static_cast<uint32_t>(sv.size()));
        (*wp->mutable_fields())["s"] = wf;
    }
    {
        std::vector<uint64_t> zz(iv.size());
        for (size_t i = 0; i < iv.size(); ++i) {
            zz[i] = (static_cast<uint64_t>(iv[i]) << 1) ^ static_cast<uint64_t>(iv[i] >> 63);
        }
        ::timestar_pb::WriteField wf;
        auto enc = IntegerEncoder::encode(std::span<const uint64_t>(zz));
        wf.mutable_int64_values()->set_compressed_ffor(reinterpret_cast<const char*>(enc.data.data()), enc.size());
        (*wp->mutable_fields())["i"] = wf;
    }

    auto bytes = serialize(req);
    auto points = parseWriteRequest(bytes.data(), bytes.size());

    ASSERT_EQ(points.size(), 1u);
    const auto& p = points[0];
    EXPECT_EQ(p.timestamps, ts);
    ASSERT_EQ(p.fields.at("d").doubles.size(), dv.size());
    for (size_t i = 0; i < dv.size(); ++i) {
        EXPECT_DOUBLE_EQ(p.fields.at("d").doubles[i], dv[i]);
    }
    ASSERT_EQ(p.fields.at("b").bools.size(), bv.size());
    for (size_t i = 0; i < bv.size(); ++i) {
        EXPECT_EQ(p.fields.at("b").bools[i] != 0, bv[i]);
    }
    EXPECT_EQ(p.fields.at("s").strings, sv);
    EXPECT_EQ(p.fields.at("i").integers, iv);
}

// ============================================================================
// Fast path: compressed write-request decode paths (Approach B)
//
// parseWriteRequestFast is the parser handleWrite actually uses for protobuf
// bodies, so these are the production-path mirrors of the generic-parser
// tests above.  Regression tests for the silent-data-loss bug where all
// compressed_* fields were ignored (compressed timestamps -> generated
// defaults; compressed values -> field silently skipped).
// ============================================================================

TEST(ProtoWriteFastPathCompressed, CompressedTimestampsDecodedNotDefaulted) {
    auto ts = makeTimestamps(200);

    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("gap_metric");
    (*wp->mutable_tags())["host"] = "server-01";

    auto encoded = IntegerEncoder::encode(std::span<const uint64_t>(ts));
    wp->set_compressed_timestamps(encoded.data.data(), encoded.size());

    ::timestar_pb::WriteField wf;
    for (size_t i = 0; i < ts.size(); ++i) {
        wf.mutable_double_values()->add_values(static_cast<double>(i) * 0.5);
    }
    (*wp->mutable_fields())["value"] = wf;

    auto bytes = serialize(req);
    // Sentinel default timestamp: if the parser ignored compressed_timestamps
    // it would generate timestamps starting from this value instead.
    const uint64_t sentinelDefault = 42ULL;
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), sentinelDefault);

    EXPECT_EQ(result.failedWrites, 0);
    ASSERT_EQ(result.inserts.size(), 1u);
    ASSERT_NE(result.inserts[0].timestamps, nullptr);
    EXPECT_EQ(*result.inserts[0].timestamps, ts);
    EXPECT_EQ(result.totalPoints, static_cast<int64_t>(ts.size()));
}

TEST(ProtoWriteFastPathCompressed, CompressedAlpDoublesRoundTrip) {
    auto ts = makeTimestamps(150);
    std::vector<double> values;
    values.reserve(ts.size());
    for (size_t i = 0; i < ts.size(); ++i) {
        values.push_back(20.0 + static_cast<double>(i) * 0.25);
    }

    ::timestar_pb::WriteRequest req;
    auto* wp = addPoint(req, ts);

    ::timestar_pb::WriteField wf;
    auto encoded = FloatEncoder::encode(std::span<const double>(values));
    wf.mutable_double_values()->set_compressed_alp(reinterpret_cast<const char*>(encoded.data.data()),
                                                   encoded.dataByteSize());
    (*wp->mutable_fields())["temp"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), 0);

    EXPECT_EQ(result.failedWrites, 0);
    EXPECT_TRUE(result.errors.empty());
    ASSERT_EQ(result.inserts.size(), 1u);
    const auto& ffi = result.inserts[0];
    EXPECT_EQ(ffi.type, FastFieldInsert::Type::DOUBLE);
    ASSERT_EQ(ffi.doubleValues.size(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_DOUBLE_EQ(ffi.doubleValues[i], values[i]) << "index " << i;
    }
    ASSERT_NE(ffi.timestamps, nullptr);
    EXPECT_EQ(*ffi.timestamps, ts);
}

TEST(ProtoWriteFastPathCompressed, CompressedRleBoolsRoundTrip) {
    auto ts = makeTimestamps(64);
    std::vector<bool> values;
    values.reserve(ts.size());
    for (size_t i = 0; i < ts.size(); ++i) {
        values.push_back(i % 3 == 0);
    }

    ::timestar_pb::WriteRequest req;
    auto* wp = addPoint(req, ts);

    ::timestar_pb::WriteField wf;
    auto encoded = BoolEncoderRLE::encode(values);
    wf.mutable_bool_values()->set_compressed_rle(reinterpret_cast<const char*>(encoded.data.data()), encoded.size());
    (*wp->mutable_fields())["flag"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), 0);

    EXPECT_EQ(result.failedWrites, 0);
    ASSERT_EQ(result.inserts.size(), 1u);
    const auto& ffi = result.inserts[0];
    EXPECT_EQ(ffi.type, FastFieldInsert::Type::BOOL);
    ASSERT_EQ(ffi.boolValues.size(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(ffi.boolValues[i] != 0, values[i]) << "index " << i;
    }
}

TEST(ProtoWriteFastPathCompressed, CompressedZstdStringsRoundTripWithCount) {
    auto ts = makeTimestamps(40);
    std::vector<std::string> values;
    values.reserve(ts.size());
    for (size_t i = 0; i < ts.size(); ++i) {
        values.push_back("status-" + std::to_string(i % 5));
    }

    ::timestar_pb::WriteRequest req;
    auto* wp = addPoint(req, ts);

    ::timestar_pb::WriteField wf;
    auto encoded = StringEncoder::encode(std::span<const std::string>(values));
    wf.mutable_string_values()->set_compressed_zstd(reinterpret_cast<const char*>(encoded.data.data()),
                                                    encoded.size());
    wf.mutable_string_values()->set_count(static_cast<uint32_t>(values.size()));
    (*wp->mutable_fields())["status"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), 0);

    EXPECT_EQ(result.failedWrites, 0);
    ASSERT_EQ(result.inserts.size(), 1u);
    const auto& ffi = result.inserts[0];
    EXPECT_EQ(ffi.type, FastFieldInsert::Type::STRING);
    EXPECT_EQ(ffi.stringValues, values);
}

TEST(ProtoWriteFastPathCompressed, CompressedZstdStringsCountFallsBackToTimestampCount) {
    // count == 0 -> the parser must fall back to timestamps.size(), the exact
    // count-resolution rule of the generic parseWriteRequest.
    auto ts = makeTimestamps(12);
    std::vector<std::string> values;
    for (size_t i = 0; i < ts.size(); ++i) {
        values.push_back("v" + std::to_string(i));
    }

    ::timestar_pb::WriteRequest req;
    auto* wp = addPoint(req, ts);

    ::timestar_pb::WriteField wf;
    auto encoded = StringEncoder::encode(std::span<const std::string>(values));
    wf.mutable_string_values()->set_compressed_zstd(reinterpret_cast<const char*>(encoded.data.data()),
                                                    encoded.size());
    // deliberately do NOT set count
    (*wp->mutable_fields())["status"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), 0);

    EXPECT_EQ(result.failedWrites, 0);
    ASSERT_EQ(result.inserts.size(), 1u);
    EXPECT_EQ(result.inserts[0].stringValues, values);
}

TEST(ProtoWriteFastPathCompressed, CompressedFforInt64ZigZagRoundTrip) {
    auto ts = makeTimestamps(100);
    std::vector<int64_t> values;
    values.reserve(ts.size());
    for (size_t i = 0; i < ts.size(); ++i) {
        values.push_back((i % 2 == 0 ? -1 : 1) * static_cast<int64_t>(i * 1000));
    }

    std::vector<uint64_t> zz(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        zz[i] = (static_cast<uint64_t>(values[i]) << 1) ^ static_cast<uint64_t>(values[i] >> 63);
    }
    auto encoded = IntegerEncoder::encode(std::span<const uint64_t>(zz));

    ::timestar_pb::WriteRequest req;
    auto* wp = addPoint(req, ts);
    ::timestar_pb::WriteField wf;
    wf.mutable_int64_values()->set_compressed_ffor(reinterpret_cast<const char*>(encoded.data.data()),
                                                   encoded.size());
    (*wp->mutable_fields())["counter"] = wf;

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), 0);

    EXPECT_EQ(result.failedWrites, 0);
    ASSERT_EQ(result.inserts.size(), 1u);
    const auto& ffi = result.inserts[0];
    EXPECT_EQ(ffi.type, FastFieldInsert::Type::INTEGER);
    EXPECT_EQ(ffi.integerValues, values);
}

TEST(ProtoWriteFastPathCompressed, AllFieldsCompressedInOnePoint) {
    auto ts = makeTimestamps(80);
    std::vector<double> dv;
    std::vector<bool> bv;
    std::vector<std::string> sv;
    std::vector<int64_t> iv;
    for (size_t i = 0; i < ts.size(); ++i) {
        dv.push_back(static_cast<double>(i) * 1.5);
        bv.push_back(i % 2 == 0);
        sv.push_back("s" + std::to_string(i % 7));
        iv.push_back(static_cast<int64_t>(i) - 40);
    }

    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("gap_metric");
    (*wp->mutable_tags())["host"] = "server-01";

    auto tsEnc = IntegerEncoder::encode(std::span<const uint64_t>(ts));
    wp->set_compressed_timestamps(tsEnc.data.data(), tsEnc.size());

    {
        ::timestar_pb::WriteField wf;
        auto enc = FloatEncoder::encode(std::span<const double>(dv));
        wf.mutable_double_values()->set_compressed_alp(reinterpret_cast<const char*>(enc.data.data()),
                                                       enc.dataByteSize());
        (*wp->mutable_fields())["d"] = wf;
    }
    {
        ::timestar_pb::WriteField wf;
        auto enc = BoolEncoderRLE::encode(bv);
        wf.mutable_bool_values()->set_compressed_rle(reinterpret_cast<const char*>(enc.data.data()), enc.size());
        (*wp->mutable_fields())["b"] = wf;
    }
    {
        ::timestar_pb::WriteField wf;
        auto enc = StringEncoder::encode(std::span<const std::string>(sv));
        wf.mutable_string_values()->set_compressed_zstd(reinterpret_cast<const char*>(enc.data.data()), enc.size());
        wf.mutable_string_values()->set_count(static_cast<uint32_t>(sv.size()));
        (*wp->mutable_fields())["s"] = wf;
    }
    {
        std::vector<uint64_t> zz(iv.size());
        for (size_t i = 0; i < iv.size(); ++i) {
            zz[i] = (static_cast<uint64_t>(iv[i]) << 1) ^ static_cast<uint64_t>(iv[i] >> 63);
        }
        ::timestar_pb::WriteField wf;
        auto enc = IntegerEncoder::encode(std::span<const uint64_t>(zz));
        wf.mutable_int64_values()->set_compressed_ffor(reinterpret_cast<const char*>(enc.data.data()), enc.size());
        (*wp->mutable_fields())["i"] = wf;
    }

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), 0);

    EXPECT_EQ(result.failedWrites, 0);
    EXPECT_TRUE(result.errors.empty());
    ASSERT_EQ(result.inserts.size(), 4u);
    EXPECT_EQ(result.totalPoints, static_cast<int64_t>(ts.size() * 4));

    const FastFieldInsert* byName[4] = {nullptr, nullptr, nullptr, nullptr};
    for (const auto& ffi : result.inserts) {
        ASSERT_NE(ffi.timestamps, nullptr);
        EXPECT_EQ(*ffi.timestamps, ts) << "field " << ffi.fieldName;
        if (ffi.fieldName == "d")
            byName[0] = &ffi;
        else if (ffi.fieldName == "b")
            byName[1] = &ffi;
        else if (ffi.fieldName == "s")
            byName[2] = &ffi;
        else if (ffi.fieldName == "i")
            byName[3] = &ffi;
    }
    ASSERT_NE(byName[0], nullptr);
    ASSERT_NE(byName[1], nullptr);
    ASSERT_NE(byName[2], nullptr);
    ASSERT_NE(byName[3], nullptr);

    ASSERT_EQ(byName[0]->doubleValues.size(), dv.size());
    for (size_t i = 0; i < dv.size(); ++i) {
        EXPECT_DOUBLE_EQ(byName[0]->doubleValues[i], dv[i]);
    }
    ASSERT_EQ(byName[1]->boolValues.size(), bv.size());
    for (size_t i = 0; i < bv.size(); ++i) {
        EXPECT_EQ(byName[1]->boolValues[i] != 0, bv[i]);
    }
    EXPECT_EQ(byName[2]->stringValues, sv);
    EXPECT_EQ(byName[3]->integerValues, iv);
}

TEST(ProtoWriteFastPathCompressed, CorruptCompressedTimestampsIsPerPointError) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("gap_metric");
    (*wp->mutable_tags())["host"] = "server-01";
    // 4 bytes cannot hold an FFOR block header -> decoder throws
    wp->set_compressed_timestamps("\x01\x02\x03\x04", 4);

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(1.0);
    (*wp->mutable_fields())["f"] = wf;

    auto bytes = serialize(req);
    FastPathResult result;
    ASSERT_NO_THROW(result = parseWriteRequestFast(bytes.data(), bytes.size(), 0));

    EXPECT_EQ(result.inserts.size(), 0u);
    EXPECT_EQ(result.failedWrites, 1);
    ASSERT_GE(result.errors.size(), 1u);
    EXPECT_NE(result.errors[0].find("Corrupt compressed timestamps"), std::string::npos) << result.errors[0];
}

TEST(ProtoWriteFastPathCompressed, CorruptCompressedValuesIsPerPointError) {
    auto ts = makeTimestamps(8);
    ::timestar_pb::WriteRequest req;
    auto* wp = addPoint(req, ts);

    // Garbage bytes with a bad ALP magic number -> decoder throws
    std::string garbage(32, '\xAB');
    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->set_compressed_alp(garbage.data(), garbage.size());
    (*wp->mutable_fields())["temp"] = wf;

    auto bytes = serialize(req);
    FastPathResult result;
    ASSERT_NO_THROW(result = parseWriteRequestFast(bytes.data(), bytes.size(), 0));

    // The only field failed -> the point counts as a failed write with an
    // error message, NOT a silent success.
    EXPECT_EQ(result.inserts.size(), 0u);
    EXPECT_EQ(result.totalPoints, 0);
    EXPECT_EQ(result.failedWrites, 1);
    ASSERT_GE(result.errors.size(), 1u);
    EXPECT_NE(result.errors[0].find("failed to decode compressed values"), std::string::npos) << result.errors[0];
}

TEST(ProtoWriteFastPathCompressed, CorruptCompressedFieldDoesNotAffectSiblingField) {
    auto ts = makeTimestamps(4);
    ::timestar_pb::WriteRequest req;
    auto* wp = addPoint(req, ts);

    // Corrupt compressed int64 field
    {
        ::timestar_pb::WriteField wf;
        wf.mutable_int64_values()->set_compressed_ffor("\x01\x02\x03", 3);
        (*wp->mutable_fields())["bad"] = wf;
    }
    // Valid plain double field
    {
        ::timestar_pb::WriteField wf;
        for (size_t i = 0; i < ts.size(); ++i) {
            wf.mutable_double_values()->add_values(static_cast<double>(i));
        }
        (*wp->mutable_fields())["good"] = wf;
    }

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), 0);

    // Good field survives; the corrupt one produces an error entry.
    ASSERT_EQ(result.inserts.size(), 1u);
    EXPECT_EQ(result.inserts[0].fieldName, "good");
    EXPECT_EQ(result.totalPoints, static_cast<int64_t>(ts.size()));
    EXPECT_EQ(result.failedWrites, 0);  // point still had a processed field
    ASSERT_GE(result.errors.size(), 1u);
    EXPECT_NE(result.errors[0].find("'bad'"), std::string::npos) << result.errors[0];
}

TEST(ProtoWriteFastPathCompressed, FastAndGenericParsersAgreeOnCompressedPoint) {
    // Both parsers must decode identical data from the same wire bytes.
    auto ts = makeTimestamps(60);
    std::vector<double> dv;
    for (size_t i = 0; i < ts.size(); ++i) {
        dv.push_back(1.25 * static_cast<double>(i));
    }

    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("gap_metric");
    (*wp->mutable_tags())["host"] = "server-01";
    auto tsEnc = IntegerEncoder::encode(std::span<const uint64_t>(ts));
    wp->set_compressed_timestamps(tsEnc.data.data(), tsEnc.size());
    ::timestar_pb::WriteField wf;
    auto enc = FloatEncoder::encode(std::span<const double>(dv));
    wf.mutable_double_values()->set_compressed_alp(reinterpret_cast<const char*>(enc.data.data()),
                                                   enc.dataByteSize());
    (*wp->mutable_fields())["value"] = wf;

    auto bytes = serialize(req);
    auto generic = parseWriteRequest(bytes.data(), bytes.size());
    auto fast = parseWriteRequestFast(bytes.data(), bytes.size(), 0);

    ASSERT_EQ(generic.size(), 1u);
    ASSERT_EQ(fast.inserts.size(), 1u);
    EXPECT_EQ(generic[0].timestamps, *fast.inserts[0].timestamps);
    EXPECT_EQ(generic[0].fields.at("value").doubles, fast.inserts[0].doubleValues);
}

// ============================================================================
// Compressed-timestamp count regression (silent truncation bug)
//
// FFOR delta-of-delta encodes regular-spaced timestamps at ~0.03 bytes/value,
// so the old bytes/2 + 1024 decode cap truncated any point with more than
// ~1052 one-second-spaced timestamps (2000 -> 1052, 300k -> 3380) while the
// write still reported full SUCCESS.  Both parsers must decode the TRUE count
// carried in the FFOR block headers, and reject (never truncate) payloads
// exceeding kMaxCompressedPointsPerWritePoint.
// ============================================================================

namespace {

// Build a WriteRequest with one point: FFOR-compressed timestamps plus one
// FFOR-compressed int64 field with a matching value count (constant values
// compress to almost nothing, keeping the big-count tests fast).
std::string buildCompressedTsRequest(const std::vector<uint64_t>& ts) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("gap_metric");
    (*wp->mutable_tags())["host"] = "server-01";

    auto tsEnc = IntegerEncoder::encode(std::span<const uint64_t>(ts));
    wp->set_compressed_timestamps(tsEnc.data.data(), tsEnc.size());

    // int64 value 7 zigzag-encoded (as the client encoder does) for every point
    std::vector<uint64_t> zz(ts.size(), (7ULL << 1));
    auto vEnc = IntegerEncoder::encode(std::span<const uint64_t>(zz));
    ::timestar_pb::WriteField wf;
    wf.mutable_int64_values()->set_compressed_ffor(reinterpret_cast<const char*>(vEnc.data.data()), vEnc.size());
    (*wp->mutable_fields())["value"] = wf;

    std::string bytes;
    req.SerializeToString(&bytes);
    return bytes;
}

}  // namespace

TEST(ProtoCompressedWriteGap, CompressedTimestamps2000NotTruncated) {
    // 2000 one-second-spaced timestamps: the old cap decoded exactly 1052.
    auto ts = makeTimestamps(2000);
    auto bytes = buildCompressedTsRequest(ts);

    auto generic = parseWriteRequest(bytes.data(), bytes.size());
    ASSERT_EQ(generic.size(), 1u);
    ASSERT_EQ(generic[0].timestamps.size(), 2000u);
    EXPECT_EQ(generic[0].timestamps, ts);
    EXPECT_EQ(generic[0].fields.at("value").integers.size(), 2000u);

    auto fast = parseWriteRequestFast(bytes.data(), bytes.size(), 0);
    EXPECT_EQ(fast.failedWrites, 0);
    ASSERT_EQ(fast.inserts.size(), 1u);
    ASSERT_EQ(fast.inserts[0].timestamps->size(), 2000u);
    EXPECT_EQ(*fast.inserts[0].timestamps, ts);
    EXPECT_EQ(fast.inserts[0].integerValues.size(), 2000u);
    EXPECT_EQ(fast.totalPoints, 2000);
}

TEST(ProtoCompressedWriteGap, CompressedTimestamps300kNotTruncated) {
    // 300k values compress to ~4.7KB; the old cap decoded exactly 3380.
    constexpr size_t kCount = 300'000;
    auto ts = makeTimestamps(kCount);
    auto bytes = buildCompressedTsRequest(ts);

    auto generic = parseWriteRequest(bytes.data(), bytes.size());
    ASSERT_EQ(generic.size(), 1u);
    ASSERT_EQ(generic[0].timestamps.size(), kCount);
    EXPECT_EQ(generic[0].timestamps.front(), ts.front());
    EXPECT_EQ(generic[0].timestamps.back(), ts.back());

    auto fast = parseWriteRequestFast(bytes.data(), bytes.size(), 0);
    EXPECT_EQ(fast.failedWrites, 0);
    ASSERT_EQ(fast.inserts.size(), 1u);
    ASSERT_EQ(fast.inserts[0].timestamps->size(), kCount);
    EXPECT_EQ(fast.inserts[0].timestamps->back(), ts.back());
    EXPECT_EQ(fast.totalPoints, static_cast<int64_t>(kCount));
}

TEST(ProtoCompressedWriteGap, CompressedTimestampsAtLimitAccepted) {
    // Exactly kMaxCompressedPointsPerWritePoint values must be accepted in full.
    const size_t kCount = kMaxCompressedPointsPerWritePoint;
    auto ts = makeTimestamps(kCount);
    auto bytes = buildCompressedTsRequest(ts);

    auto generic = parseWriteRequest(bytes.data(), bytes.size());
    ASSERT_EQ(generic.size(), 1u);
    EXPECT_EQ(generic[0].timestamps.size(), kCount);
    EXPECT_EQ(generic[0].timestamps.back(), ts.back());

    auto fast = parseWriteRequestFast(bytes.data(), bytes.size(), 0);
    EXPECT_EQ(fast.failedWrites, 0);
    ASSERT_EQ(fast.inserts.size(), 1u);
    EXPECT_EQ(fast.inserts[0].timestamps->size(), kCount);
}

TEST(ProtoCompressedWriteGap, CompressedTimestampsOverLimitRejectedNotTruncated) {
    // One value past the limit: a loud rejection, never a stored prefix.
    const size_t kCount = kMaxCompressedPointsPerWritePoint + 1;
    auto ts = makeTimestamps(kCount);
    auto bytes = buildCompressedTsRequest(ts);

    // Generic parser: whole-request error.
    EXPECT_THROW(parseWriteRequest(bytes.data(), bytes.size()), std::runtime_error);

    // Fast path: clean per-point error, nothing emitted for the point.
    auto fast = parseWriteRequestFast(bytes.data(), bytes.size(), 0);
    EXPECT_EQ(fast.inserts.size(), 0u);
    EXPECT_EQ(fast.totalPoints, 0);
    EXPECT_EQ(fast.failedWrites, 1);
    ASSERT_GE(fast.errors.size(), 1u);
    EXPECT_NE(fast.errors[0].find("max points per write point"), std::string::npos) << fast.errors[0];
}

// ============================================================================
// Generic parser: unset oneof field is skipped
// ============================================================================

TEST(ProtoConverterGap, GenericParserSkipsUnsetOneofField) {
    auto ts = makeTimestamps(3);
    ::timestar_pb::WriteRequest req;
    auto* wp = addPoint(req, ts);

    ::timestar_pb::WriteField emptyField;  // no typed_values set
    (*wp->mutable_fields())["ghost"] = emptyField;

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(1.0);
    wf.mutable_double_values()->add_values(2.0);
    wf.mutable_double_values()->add_values(3.0);
    (*wp->mutable_fields())["real"] = wf;

    auto bytes = serialize(req);
    auto points = parseWriteRequest(bytes.data(), bytes.size());

    ASSERT_EQ(points.size(), 1u);
    EXPECT_EQ(points[0].fields.count("ghost"), 0u);
    EXPECT_EQ(points[0].fields.count("real"), 1u);
}

// ============================================================================
// formatQueryResponse error branch
// ============================================================================

TEST(ProtoConverterGap, FormatQueryResponseErrorBranch) {
    QueryResponseData response;
    response.success = false;
    response.errorCode = "QUERY_TIMEOUT";
    response.errorMessage = "query exceeded time budget";

    auto bytes = formatQueryResponse(response);

    ::timestar_pb::QueryResponse resp;
    ASSERT_TRUE(resp.ParseFromString(bytes));
    EXPECT_EQ(resp.status(), "error");
    EXPECT_EQ(resp.error_code(), "QUERY_TIMEOUT");
    EXPECT_EQ(resp.error_message(), "query exceeded time budget");
    EXPECT_EQ(resp.series_size(), 0);
}

// ============================================================================
// Parse-failure branches of the non-write parsers
// ============================================================================

TEST(ProtoConverterGap, ParseFunctionsThrowOnCorruptBytes) {
    // Field 1, wire type 2 (length-delimited), declared length 0x7F but the
    // buffer ends immediately after — guaranteed ParseFromArray failure.
    const char corrupt[] = {'\x0A', '\x7F'};
    const size_t n = sizeof(corrupt);

    EXPECT_THROW(parseQueryRequest(corrupt, n), std::runtime_error);
    EXPECT_THROW(parseBatchDeleteRequest(corrupt, n), std::runtime_error);
    EXPECT_THROW(parseSingleDeleteRequest(corrupt, n), std::runtime_error);
    EXPECT_THROW(parseRetentionPutRequest(corrupt, n), std::runtime_error);
    EXPECT_THROW(parseSubscribeRequest(corrupt, n), std::runtime_error);
    EXPECT_THROW(parseDerivedQueryRequest(corrupt, n), std::runtime_error);
    EXPECT_THROW(parseWriteRequest(corrupt, n), std::runtime_error);
    EXPECT_THROW(parseWriteRequestFast(corrupt, n, 0), std::runtime_error);
}

// ============================================================================
// Fast path: invalid tag VALUE (key validation is covered elsewhere)
// ============================================================================

TEST(ProtoWriteFastPathGap, InvalidTagValueRejectsPoint) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("m");
    (*wp->mutable_tags())["host"] = std::string("bad") + '\0' + "value";  // NUL is invalid

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(1.0);
    (*wp->mutable_fields())["f"] = wf;
    wp->add_timestamps(1704067200000000000ULL);

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), 0);

    EXPECT_EQ(result.inserts.size(), 0u);
    EXPECT_EQ(result.failedWrites, 1);
    ASSERT_EQ(result.errors.size(), 1u);
    EXPECT_NE(result.errors[0].find("Invalid tag value"), std::string::npos) << result.errors[0];
}

// '=' in a tag value is now accepted — buildSeriesKey escapes it, so the key
// stays unambiguous.  (Previously this was rejected.)
TEST(ProtoWriteFastPathGap, TagValueWithEqualsAccepted) {
    ::timestar_pb::WriteRequest req;
    auto* wp = req.add_writes();
    wp->set_measurement("m");
    (*wp->mutable_tags())["host"] = "a=b";

    ::timestar_pb::WriteField wf;
    wf.mutable_double_values()->add_values(1.0);
    (*wp->mutable_fields())["f"] = wf;
    wp->add_timestamps(1704067200000000000ULL);

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), 0);

    EXPECT_EQ(result.inserts.size(), 1u);
    EXPECT_EQ(result.failedWrites, 0);
    EXPECT_TRUE(result.errors.empty());
}

// ============================================================================
// Fast path: error list capped at 10 entries
// ============================================================================

TEST(ProtoWriteFastPathGap, ErrorListCappedAtTen) {
    ::timestar_pb::WriteRequest req;
    const int kBadPoints = 25;
    for (int i = 0; i < kBadPoints; ++i) {
        auto* wp = req.add_writes();
        wp->set_measurement(std::string("bad") + '\0');  // NUL is invalid
        ::timestar_pb::WriteField wf;
        wf.mutable_double_values()->add_values(1.0);
        (*wp->mutable_fields())["f"] = wf;
        wp->add_timestamps(1704067200000000000ULL);
    }

    auto bytes = serialize(req);
    auto result = parseWriteRequestFast(bytes.data(), bytes.size(), 0);

    EXPECT_EQ(result.failedWrites, kBadPoints);  // all counted...
    EXPECT_EQ(result.errors.size(), 10u);        // ...but messages capped
    EXPECT_EQ(result.inserts.size(), 0u);
    EXPECT_EQ(result.totalPoints, 0);
}

// ============================================================================
// Truncated-byte robustness: parsers must throw or return cleanly, not crash
// ============================================================================

TEST(ProtoWriteGap, TruncatedBytesNeverCrash) {
    // Build a rich request (plain + compressed fields) and feed every prefix
    // of its serialization to both parsers.  Truncated protobuf must either
    // throw std::runtime_error (parse failure) or yield a clean partial result.
    auto ts = makeTimestamps(50);
    std::vector<double> dv(ts.size(), 3.14);
    std::vector<int64_t> iv(ts.size(), -7);

    ::timestar_pb::WriteRequest req;
    auto* wp = addPoint(req, ts);
    {
        ::timestar_pb::WriteField wf;
        auto enc = FloatEncoder::encode(std::span<const double>(dv));
        wf.mutable_double_values()->set_compressed_alp(reinterpret_cast<const char*>(enc.data.data()),
                                                       enc.dataByteSize());
        (*wp->mutable_fields())["d"] = wf;
    }
    {
        ::timestar_pb::WriteField wf;
        for (int64_t v : iv) {
            wf.mutable_int64_values()->add_values(v);
        }
        (*wp->mutable_fields())["i"] = wf;
    }
    auto* wp2 = req.add_writes();
    wp2->set_measurement("second");
    ::timestar_pb::WriteField wf2;
    wf2.mutable_string_values()->add_values("hello");
    (*wp2->mutable_fields())["s"] = wf2;
    wp2->add_timestamps(1704067200000000000ULL);

    auto bytes = serialize(req);
    ASSERT_GT(bytes.size(), 100u);

    size_t genericThrows = 0;
    size_t fastThrows = 0;
    for (size_t len = 0; len <= bytes.size(); ++len) {
        try {
            auto points = parseWriteRequest(bytes.data(), len);
            (void)points;
        } catch (const std::runtime_error&) {
            ++genericThrows;
        }
        try {
            auto result = parseWriteRequestFast(bytes.data(), len, 1704067200000000000ULL);
            (void)result;
        } catch (const std::runtime_error&) {
            ++fastThrows;
        }
    }
    // Sanity: at least some prefixes must be rejected as corrupt
    EXPECT_GT(genericThrows, 0u);
    EXPECT_GT(fastThrows, 0u);
}
