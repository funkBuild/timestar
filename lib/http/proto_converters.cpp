// proto_converters.cpp -- Bidirectional converters between Protobuf and C++ types.
//
// This translation unit ONLY includes timestar.pb.h (the protobuf-generated header)
// and proto_converters.hpp (which defines intermediate types).  It does NOT include
// any internal headers whose types collide with proto-generated names (e.g.
// QueryResponse, StreamingBatch, SubscriptionStats, etc.).

#include "proto_converters.hpp"

#include "timestar.pb.h"

// Compression encoders for Approach B compressed proto fields
#include "../encoding/bool_encoder_rle.hpp"
#include "../encoding/float_encoder.hpp"
#include "../encoding/integer_encoder.hpp"
#include "../encoding/string_encoder.hpp"

#include <climits>
#include <stdexcept>

namespace {
// Guard against size_t > INT_MAX truncation when calling protobuf ParseFromArray.
inline int safeProtoSize(size_t size) {
    if (size > static_cast<size_t>(INT_MAX)) {
        throw std::runtime_error("Protobuf payload too large");
    }
    return static_cast<int>(size);
}
}  // namespace

#include <cmath>
#include <limits>
#include <stdexcept>

namespace timestar::proto {

// ============================================================================
// Write converters
// ============================================================================

std::vector<MultiWritePoint> parseWriteRequest(const void* data, size_t size) {
    ::timestar_pb::WriteRequest req;
    if (!req.ParseFromArray(data, safeProtoSize(size))) {
        throw std::runtime_error("Failed to parse WriteRequest protobuf");
    }

    std::vector<MultiWritePoint> points;
    points.reserve(req.writes_size());

    for (const auto& wp : req.writes()) {
        MultiWritePoint mwp;
        mwp.measurement = wp.measurement();

        for (const auto& [k, v] : wp.tags()) {
            mwp.tags[k] = v;
        }

        // Check for compressed timestamps first (Approach B)
        if (!wp.compressed_timestamps().empty()) {
            const auto& ct = wp.compressed_timestamps();
            Slice tsSlice(reinterpret_cast<const uint8_t*>(ct.data()), ct.size());
            // Upper bound: compressed data can't encode more values than bytes/2
            // (minimum 16 bytes per block header). The decoder stops at slice end.
            size_t maxCount = ct.size() / 2 + 1024;
            IntegerEncoder::decode(tsSlice, static_cast<unsigned int>(maxCount), mwp.timestamps);
        } else {
            mwp.timestamps.reserve(wp.timestamps_size());
            for (int i = 0; i < wp.timestamps_size(); ++i) {
                mwp.timestamps.push_back(wp.timestamps(i));
            }
        }

        for (const auto& [fieldName, writeField] : wp.fields()) {
            FieldArrays fa;
            switch (writeField.typed_values_case()) {
                case ::timestar_pb::WriteField::kDoubleValues: {
                    fa.type = FieldArrays::DOUBLE;
                    const auto& dv = writeField.double_values();
                    if (!dv.compressed_alp().empty()) {
                        const auto& c = dv.compressed_alp();
                        CompressedSlice cs(reinterpret_cast<const uint8_t*>(c.data()), c.size());
                        FloatDecoder::decode(cs, 0, mwp.timestamps.size(), fa.doubles);
                    } else {
                        const auto& vals = dv.values();
                        fa.doubles.reserve(vals.size());
                        for (double v : vals)
                            fa.doubles.push_back(v);
                    }
                    break;
                }
                case ::timestar_pb::WriteField::kBoolValues: {
                    fa.type = FieldArrays::BOOL;
                    const auto& bv = writeField.bool_values();
                    if (!bv.compressed_rle().empty()) {
                        const auto& c = bv.compressed_rle();
                        Slice bSlice(reinterpret_cast<const uint8_t*>(c.data()), c.size());
                        std::vector<bool> boolVec;
                        BoolEncoderRLE::decode(bSlice, 0, mwp.timestamps.size(), boolVec);
                        fa.bools.reserve(boolVec.size());
                        for (bool b : boolVec)
                            fa.bools.push_back(b ? 1 : 0);
                    } else {
                        const auto& vals = bv.values();
                        fa.bools.reserve(vals.size());
                        for (bool v : vals)
                            fa.bools.push_back(v ? 1 : 0);
                    }
                    break;
                }
                case ::timestar_pb::WriteField::kStringValues: {
                    fa.type = FieldArrays::STRING;
                    const auto& sv = writeField.string_values();
                    if (!sv.compressed_zstd().empty()) {
                        const auto& c = sv.compressed_zstd();
                        Slice sSlice(reinterpret_cast<const uint8_t*>(c.data()), c.size());
                        size_t count = sv.count() > 0 ? sv.count() : mwp.timestamps.size();
                        StringEncoder::decode(sSlice, count, fa.strings);
                    } else {
                        const auto& vals = sv.values();
                        fa.strings.reserve(vals.size());
                        for (const auto& v : vals)
                            fa.strings.push_back(v);
                    }
                    break;
                }
                case ::timestar_pb::WriteField::kInt64Values: {
                    fa.type = FieldArrays::INTEGER;
                    const auto& iv = writeField.int64_values();
                    if (!iv.compressed_ffor().empty()) {
                        const auto& c = iv.compressed_ffor();
                        Slice iSlice(reinterpret_cast<const uint8_t*>(c.data()), c.size());
                        std::vector<uint64_t> decoded;
                        IntegerEncoder::decode(iSlice, mwp.timestamps.size(), decoded);
                        fa.integers.reserve(decoded.size());
                        // Reverse zigzag: the encoder zigzag'd int64 -> uint64 before FFOR
                        for (auto v : decoded) {
                            fa.integers.push_back(static_cast<int64_t>((v >> 1) ^ -(v & 1)));
                        }
                    } else {
                        const auto& vals = iv.values();
                        fa.integers.reserve(vals.size());
                        for (int64_t v : vals)
                            fa.integers.push_back(v);
                    }
                    break;
                }
                default:
                    continue;
            }
            mwp.fields[fieldName] = std::move(fa);
        }

        points.push_back(std::move(mwp));
    }

    return points;
}

std::string formatWriteResponse(const std::string& status, int64_t pointsWritten, int64_t failedWrites,
                                const std::vector<std::string>& errors) {
    ::timestar_pb::WriteResponse resp;
    resp.set_status(status);
    resp.set_points_written(pointsWritten);
    resp.set_failed_writes(failedWrites);
    for (const auto& err : errors) {
        resp.add_errors(err);
    }

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

// ============================================================================
// Query converters
// ============================================================================

ParsedQueryRequest parseQueryRequest(const void* data, size_t size) {
    ::timestar_pb::QueryRequest req;
    if (!req.ParseFromArray(data, safeProtoSize(size))) {
        throw std::runtime_error("Failed to parse QueryRequest protobuf");
    }

    ParsedQueryRequest result;
    result.query = req.query();
    result.startTime = req.start_time();
    result.endTime = req.end_time();
    result.aggregationInterval = req.aggregation_interval();
    return result;
}

std::string formatQueryResponse(QueryResponseData& response) {
    ::timestar_pb::QueryResponse resp;

    if (response.success) {
        resp.set_status("success");
    } else {
        resp.set_status("error");
        resp.set_error_code(response.errorCode);
        resp.set_error_message(response.errorMessage);
    }

    for (auto& sr : response.series) {
        auto* pbSeries = resp.add_series();
        pbSeries->set_measurement(sr.measurement);

        auto* pbTags = pbSeries->mutable_tags();
        for (const auto& [k, v] : sr.tags) {
            (*pbTags)[k] = v;
        }

        auto* pbFields = pbSeries->mutable_fields();
        for (auto& [fieldName, fieldData] : sr.fields) {
            auto& [timestamps, values] = fieldData;
            ::timestar_pb::FieldData fd;

            // Compress timestamps with FFOR
            if (!timestamps.empty()) {
                auto tsEncoded = IntegerEncoder::encode(std::span<const uint64_t>(timestamps));
                fd.set_compressed_timestamps(tsEncoded.data.data(), tsEncoded.size());
            }

            // Compress values with type-appropriate codec
            std::visit(
                [&fd](auto& vec) {
                    using T = std::decay_t<decltype(vec)>;
                    if constexpr (std::is_same_v<T, std::vector<double>>) {
                        if (!vec.empty()) {
                            auto encoded = FloatEncoder::encode(std::span<const double>(vec));
                            fd.mutable_double_values()->set_compressed_alp(encoded.data.data(), encoded.dataByteSize());
                        }
                    } else if constexpr (std::is_same_v<T, std::vector<bool>>) {
                        if (!vec.empty()) {
                            auto encoded = BoolEncoderRLE::encode(vec);
                            fd.mutable_bool_values()->set_compressed_rle(encoded.data.data(), encoded.size());
                        }
                    } else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
                        if (!vec.empty()) {
                            auto encoded = StringEncoder::encode(std::span<const std::string>(vec));
                            fd.mutable_string_values()->set_compressed_zstd(encoded.data.data(), encoded.size());
                            fd.mutable_string_values()->set_count(static_cast<uint32_t>(vec.size()));
                        }
                    } else if constexpr (std::is_same_v<T, std::vector<int64_t>>) {
                        if (!vec.empty()) {
                            // ZigZag encode then FFOR
                            std::vector<uint64_t> zz(vec.size());
                            for (size_t i = 0; i < vec.size(); ++i) {
                                zz[i] = (static_cast<uint64_t>(vec[i]) << 1) ^ static_cast<uint64_t>(vec[i] >> 63);
                            }
                            auto encoded = IntegerEncoder::encode(std::span<const uint64_t>(zz));
                            fd.mutable_int64_values()->set_compressed_ffor(encoded.data.data(), encoded.size());
                        }
                    }
                },
                values);

            (*pbFields)[fieldName] = std::move(fd);
        }
    }

    auto* stats = resp.mutable_statistics();
    stats->set_series_count(response.statistics.seriesCount);
    stats->set_point_count(response.statistics.pointCount);
    stats->set_execution_time_ms(response.statistics.executionTimeMs);
    for (int shard : response.statistics.shardsQueried) {
        stats->add_shards_queried(shard);
    }
    stats->set_failed_series_count(response.statistics.failedSeriesCount);
    stats->set_truncated(response.statistics.truncated);
    if (!response.statistics.truncationReason.empty()) {
        stats->set_truncation_reason(response.statistics.truncationReason);
    }

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

std::string formatQueryError(const std::string& code, const std::string& message) {
    ::timestar_pb::QueryResponse resp;
    resp.set_status("error");
    resp.set_error_code(code);
    resp.set_error_message(message);

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

// ============================================================================
// Delete converters
// ============================================================================

static ParsedDeleteRequest convertDeleteProto(const ::timestar_pb::DeleteRequest& dr) {
    ParsedDeleteRequest req;
    req.isPattern = false;

    if (!dr.series().empty()) {
        req.seriesKey = dr.series();
        req.isStructured = false;
    } else if (!dr.measurement().empty()) {
        req.measurement = dr.measurement();
        req.isStructured = true;

        for (const auto& [k, v] : dr.tags()) {
            req.tags[k] = v;
        }

        if (!dr.field().empty()) {
            req.field = dr.field();
            req.fields.push_back(req.field);
        } else if (dr.fields_size() > 0) {
            req.isPattern = true;
            for (const auto& f : dr.fields()) {
                req.fields.push_back(f);
            }
            if (!req.fields.empty()) {
                req.field = req.fields[0];
            }
        } else {
            req.isPattern = true;
        }
    }

    req.startTime = dr.start_time();
    req.endTime = dr.end_time();
    if (req.endTime == 0) {
        req.endTime = UINT64_MAX;
    }

    return req;
}

std::vector<ParsedDeleteRequest> parseBatchDeleteRequest(const void* data, size_t size) {
    ::timestar_pb::BatchDeleteRequest req;
    if (!req.ParseFromArray(data, safeProtoSize(size))) {
        throw std::runtime_error("Failed to parse BatchDeleteRequest protobuf");
    }

    std::vector<ParsedDeleteRequest> result;
    result.reserve(req.deletes_size());
    for (const auto& dr : req.deletes()) {
        result.push_back(convertDeleteProto(dr));
    }
    return result;
}

ParsedDeleteRequest parseSingleDeleteRequest(const void* data, size_t size) {
    ::timestar_pb::DeleteRequest dr;
    if (!dr.ParseFromArray(data, safeProtoSize(size))) {
        throw std::runtime_error("Failed to parse DeleteRequest protobuf");
    }
    return convertDeleteProto(dr);
}

std::string formatDeleteResponse(const std::string& status, uint64_t deletedCount, uint64_t totalRequests,
                                 const std::string& errorMessage) {
    ::timestar_pb::DeleteResponse resp;
    resp.set_status(status);
    resp.set_deleted_count(deletedCount);
    resp.set_total_requests(totalRequests);
    if (!errorMessage.empty()) {
        resp.set_error_message(errorMessage);
    }

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

// ============================================================================
// Metadata converters
// ============================================================================

std::string formatMeasurementsResponse(const std::vector<std::string>& measurements, size_t total) {
    ::timestar_pb::MeasurementsResponse resp;
    resp.set_status("success");
    for (const auto& m : measurements) {
        resp.add_measurements(m);
    }
    resp.set_total(total);

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

std::string formatTagsResponse(const std::string& measurement,
                               const std::unordered_map<std::string, std::vector<std::string>>& tags) {
    ::timestar_pb::TagsResponse resp;
    resp.set_status("success");
    resp.set_measurement(measurement);

    auto* pbTags = resp.mutable_tags();
    for (const auto& [tagKey, tagValues] : tags) {
        ::timestar_pb::TagValues tv;
        for (const auto& val : tagValues) {
            tv.add_values(val);
        }
        (*pbTags)[tagKey] = std::move(tv);
    }

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

std::string formatFieldsResponse(const std::string& measurement,
                                 const std::unordered_map<std::string, std::string>& fields) {
    ::timestar_pb::FieldsResponse resp;
    resp.set_status("success");
    resp.set_measurement(measurement);

    for (const auto& [name, type] : fields) {
        auto* fi = resp.add_fields();
        fi->set_name(name);
        fi->set_type(type);
    }

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

std::string formatCardinalityResponse(const std::string& measurement, double estimatedSeriesCount,
                                      const std::unordered_map<std::string, double>& tagCardinalities) {
    ::timestar_pb::CardinalityResponse resp;
    resp.set_status("success");
    resp.set_measurement(measurement);
    resp.set_estimated_series_count(estimatedSeriesCount);

    for (const auto& [tagKey, count] : tagCardinalities) {
        auto* tc = resp.add_tag_cardinalities();
        tc->set_tag_key(tagKey);
        tc->set_estimated_count(count);
    }

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

// ============================================================================
// Retention converters
// ============================================================================

ParsedRetentionPutRequest parseRetentionPutRequest(const void* data, size_t size) {
    ::timestar_pb::RetentionPutRequest req;
    if (!req.ParseFromArray(data, safeProtoSize(size))) {
        throw std::runtime_error("Failed to parse RetentionPutRequest protobuf");
    }

    ParsedRetentionPutRequest result;
    result.measurement = req.measurement();
    result.ttl = req.ttl();

    if (req.has_downsample()) {
        ParsedRetentionPutRequest::DownsampleData ds;
        ds.after = req.downsample().after();
        ds.afterNanos = req.downsample().after_nanos();
        ds.interval = req.downsample().interval();
        ds.intervalNanos = req.downsample().interval_nanos();
        ds.method = req.downsample().method();
        result.downsample = std::move(ds);
    }

    return result;
}

std::string formatRetentionGetResponse(const RetentionPolicyData& policy) {
    ::timestar_pb::RetentionGetResponse resp;
    resp.set_status("success");

    auto* pbPolicy = resp.mutable_policy();
    pbPolicy->set_measurement(policy.measurement);
    pbPolicy->set_ttl(policy.ttl);
    pbPolicy->set_ttl_nanos(policy.ttlNanos);

    if (policy.downsample.has_value()) {
        auto* ds = pbPolicy->mutable_downsample();
        ds->set_after(policy.downsample->after);
        ds->set_after_nanos(policy.downsample->afterNanos);
        ds->set_interval(policy.downsample->interval);
        ds->set_interval_nanos(policy.downsample->intervalNanos);
        ds->set_method(policy.downsample->method);
    }

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

std::string formatStatusResponse(const std::string& status, const std::string& message, const std::string& code) {
    ::timestar_pb::StatusResponse resp;
    resp.set_status(status);
    if (!message.empty()) {
        resp.set_message(message);
    }
    if (!code.empty()) {
        resp.set_code(code);
    }

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

// ============================================================================
// Streaming converters
// ============================================================================

ParsedSubscribeRequest parseSubscribeRequest(const void* data, size_t size) {
    ::timestar_pb::SubscribeRequest req;
    if (!req.ParseFromArray(data, safeProtoSize(size))) {
        throw std::runtime_error("Failed to parse SubscribeRequest protobuf");
    }

    ParsedSubscribeRequest result;
    result.query = req.query();
    result.formula = req.formula();
    result.startTime = req.start_time();
    result.backfill = req.backfill();
    result.aggregationInterval = req.aggregation_interval();

    for (const auto& entry : req.queries()) {
        result.queries.push_back({entry.query(), entry.label()});
    }

    return result;
}

std::string formatStreamingBatch(const StreamingBatchData& batch) {
    ::timestar_pb::StreamingBatch pbBatch;
    pbBatch.set_sequence_id(batch.sequenceId);
    pbBatch.set_label(batch.label);
    pbBatch.set_is_drop(batch.isDrop);
    pbBatch.set_dropped_count(batch.droppedCount);

    for (const auto& pt : batch.points) {
        auto* pbPt = pbBatch.add_points();
        pbPt->set_measurement(pt.measurement);
        pbPt->set_field(pt.field);
        pbPt->set_timestamp(pt.timestamp);

        auto* pbTags = pbPt->mutable_tags();
        for (const auto& [k, v] : pt.tags) {
            (*pbTags)[k] = v;
        }

        auto* pbVal = pbPt->mutable_value();
        std::visit(
            [pbVal](const auto& val) {
                using VT = std::decay_t<decltype(val)>;
                if constexpr (std::is_same_v<VT, double>) {
                    pbVal->set_double_value(val);
                } else if constexpr (std::is_same_v<VT, bool>) {
                    pbVal->set_bool_value(val);
                } else if constexpr (std::is_same_v<VT, std::string>) {
                    pbVal->set_string_value(val);
                } else if constexpr (std::is_same_v<VT, int64_t>) {
                    pbVal->set_int64_value(val);
                }
            },
            pt.value);
    }

    std::string out;
    pbBatch.SerializeToString(&out);
    return out;
}

std::string formatSubscriptionsResponse(const std::vector<SubscriptionStatsData>& subscriptions) {
    ::timestar_pb::SubscriptionsResponse resp;
    resp.set_status("success");

    for (const auto& sub : subscriptions) {
        auto* pbSub = resp.add_subscriptions();
        pbSub->set_id(sub.id);
        pbSub->set_measurement(sub.measurement);
        pbSub->set_label(sub.label);
        pbSub->set_handler_shard(sub.handlerShard);
        pbSub->set_queue_depth(sub.queueDepth);
        pbSub->set_queue_capacity(sub.queueCapacity);
        pbSub->set_dropped_points(sub.droppedPoints);
        pbSub->set_events_sent(sub.eventsSent);

        auto* pbScopes = pbSub->mutable_scopes();
        for (const auto& [k, v] : sub.scopes) {
            (*pbScopes)[k] = v;
        }

        for (const auto& f : sub.fields) {
            pbSub->add_fields(f);
        }
    }

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

// ============================================================================
// Derived query converters
// ============================================================================

ParsedDerivedQueryRequest parseDerivedQueryRequest(const void* data, size_t size) {
    ::timestar_pb::DerivedQueryRequest req;
    if (!req.ParseFromArray(data, safeProtoSize(size))) {
        throw std::runtime_error("Failed to parse DerivedQueryRequest protobuf");
    }

    ParsedDerivedQueryRequest result;
    result.formula = req.formula();
    result.startTime = req.start_time();
    result.endTime = req.end_time();
    result.aggregationInterval = req.aggregation_interval();

    for (const auto& sub : req.queries()) {
        result.queries[sub.name()] = sub.query();
    }

    return result;
}

std::string formatDerivedQueryResponse(const DerivedQueryResultData& result) {
    ::timestar_pb::DerivedQueryResponse resp;
    resp.set_status("success");
    resp.set_formula(result.formula);

    // Compress timestamps with FFOR
    if (!result.timestamps.empty()) {
        auto tsEncoded = IntegerEncoder::encode(std::span<const uint64_t>(result.timestamps));
        resp.set_compressed_timestamps(tsEncoded.data.data(), tsEncoded.size());
    }
    // Compress values with ALP
    if (!result.values.empty()) {
        auto vEncoded = FloatEncoder::encode(std::span<const double>(result.values));
        resp.set_compressed_values(vEncoded.data.data(), vEncoded.dataByteSize());
    }

    auto* stats = resp.mutable_statistics();
    stats->set_point_count(result.stats.pointCount);
    stats->set_execution_time_ms(result.stats.executionTimeMs);
    stats->set_sub_queries_executed(result.stats.subQueriesExecuted);
    stats->set_points_dropped_due_to_alignment(result.stats.pointsDroppedDueToAlignment);

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

std::string formatAnomalyResponse(const AnomalyQueryResultData& result) {
    ::timestar_pb::AnomalyResponse resp;
    resp.set_status(result.success ? "success" : "error");

    if (!result.errorMessage.empty()) {
        resp.set_error_message(result.errorMessage);
    }

    // Compress times with FFOR
    if (!result.times.empty()) {
        auto tsEncoded = IntegerEncoder::encode(std::span<const uint64_t>(result.times));
        resp.set_compressed_times(tsEncoded.data.data(), tsEncoded.size());
    }

    for (const auto& piece : result.series) {
        auto* pbPiece = resp.add_series();
        pbPiece->set_piece(piece.piece);

        for (const auto& gt : piece.groupTags) {
            pbPiece->add_group_tags(gt);
        }

        // Compress values with ALP
        if (!piece.values.empty()) {
            auto vEncoded = FloatEncoder::encode(std::span<const double>(piece.values));
            pbPiece->set_compressed_values(vEncoded.data.data(), vEncoded.dataByteSize());
        }

        if (piece.alertValue.has_value()) {
            pbPiece->set_alert_value(*piece.alertValue);
            pbPiece->set_has_alert(true);
        } else {
            pbPiece->set_has_alert(false);
        }
    }

    auto* stats = resp.mutable_statistics();
    stats->set_algorithm(result.statistics.algorithm);
    stats->set_bounds(result.statistics.bounds);
    stats->set_seasonality(result.statistics.seasonality);
    stats->set_anomaly_count(result.statistics.anomalyCount);
    stats->set_total_points(result.statistics.totalPoints);
    stats->set_execution_time_ms(result.statistics.executionTimeMs);

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

std::string formatForecastResponse(const ForecastQueryResultData& result) {
    ::timestar_pb::ForecastResponse resp;
    resp.set_status(result.success ? "success" : "error");

    if (!result.errorMessage.empty()) {
        resp.set_error_message(result.errorMessage);
    }

    // Compress times with FFOR
    if (!result.times.empty()) {
        auto tsEncoded = IntegerEncoder::encode(std::span<const uint64_t>(result.times));
        resp.set_compressed_times(tsEncoded.data.data(), tsEncoded.size());
    }

    resp.set_forecast_start_index(result.forecastStartIndex);

    for (const auto& piece : result.series) {
        auto* pbPiece = resp.add_series();
        pbPiece->set_piece(piece.piece);

        for (const auto& gt : piece.groupTags) {
            pbPiece->add_group_tags(gt);
        }

        // Materialize optional<double> as doubles (NaN for missing) then compress
        std::vector<double> raw_values;
        raw_values.reserve(piece.values.size());
        for (const auto& v : piece.values) {
            raw_values.push_back(v.has_value() ? *v : std::numeric_limits<double>::quiet_NaN());
        }
        if (!raw_values.empty()) {
            auto vEncoded = FloatEncoder::encode(std::span<const double>(raw_values));
            pbPiece->set_compressed_values(vEncoded.data.data(), vEncoded.dataByteSize());
        }
    }

    auto* stats = resp.mutable_statistics();
    stats->set_algorithm(result.statistics.algorithm);
    stats->set_deviations(result.statistics.deviations);
    stats->set_seasonality(result.statistics.seasonality);
    stats->set_slope(result.statistics.slope);
    stats->set_intercept(result.statistics.intercept);
    stats->set_r_squared(result.statistics.rSquared);
    stats->set_residual_std_dev(result.statistics.residualStdDev);
    stats->set_historical_points(result.statistics.historicalPoints);
    stats->set_forecast_points(result.statistics.forecastPoints);
    stats->set_series_count(result.statistics.seriesCount);
    stats->set_execution_time_ms(result.statistics.executionTimeMs);

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

std::string formatDerivedQueryError(const std::string& code, const std::string& message) {
    ::timestar_pb::DerivedQueryResponse resp;
    resp.set_status("error");
    resp.set_error_code(code);
    resp.set_error_message(message);

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

// ============================================================================
// Health / generic error
// ============================================================================

std::string formatHealthResponse(const std::string& status) {
    ::timestar_pb::HealthResponse resp;
    resp.set_status(status);

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

std::string formatErrorResponse(const std::string& message, const std::string& code) {
    ::timestar_pb::StatusResponse resp;
    resp.set_status("error");
    resp.set_message(message);
    if (!code.empty()) {
        resp.set_code(code);
    }

    std::string out;
    resp.SerializeToString(&out);
    return out;
}

}  // namespace timestar::proto
