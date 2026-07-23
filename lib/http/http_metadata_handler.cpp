#include "http_metadata_handler.hpp"

#include "content_negotiation.hpp"
#include "http_auth.hpp"
#include "http_error.hpp"
#include "http_response.hpp"
#include "http_routes.hpp"
#include "native_index.hpp"
#include "placement_table.hpp"  // routeToCore for the /series owner lookup
#include "proto_converters.hpp"
#include "scatter_gather.hpp"
#include "value_type_dispatch.hpp"  // valueTypeName

#include <glaze/json.hpp>

#include <algorithm>
#include <optional>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>
#include <stdexcept>
#include <unordered_map>

namespace timestar::http {

// Response struct definitions for Glaze serialization
struct MeasurementsResponse {
    std::vector<std::string> measurements;
    size_t total;
};

struct TagValuesResponse {
    std::string measurement;
    std::string tag;
    std::vector<std::string> values;
};

struct TagsResponse {
    std::string measurement;
    std::unordered_map<std::string, std::vector<std::string>> tags;
};

struct CardinalityResponse {
    std::string measurement;
    double estimated_series_count;
    std::unordered_map<std::string, double> tag_cardinalities;
};

struct FieldInfo {
    std::string name;
    std::string type;
};

// GET /series?id= — resolves an opaque SeriesId128 to something actionable.
// value_type is empty when the series has no binding yet.
struct SeriesLookupResponse {
    std::string series_id;
    std::string measurement;
    std::string field;
    std::map<std::string, std::string> tags;
    std::string value_type;
    unsigned shard;
};

struct FieldsResponse {
    std::string measurement;
    std::unordered_map<std::string, FieldInfo> fields;
    std::optional<std::unordered_map<std::string, std::string>> filtered_by;
};

// static
size_t HttpMetadataHandler::parsePaginationParam(const std::string& str, const std::string& paramName,
                                                 size_t defaultValue) {
    if (str.empty()) {
        return defaultValue;
    }
    // Reject strings with leading whitespace (e.g. " -5") since stoul silently
    // skips whitespace, which could bypass the minus-sign check below.
    if (std::isspace(static_cast<unsigned char>(str[0]))) {
        throw BadRequestException("Invalid " + paramName + " parameter: must not have leading whitespace");
    }
    // Reject strings that start with '-' since stoul would throw std::invalid_argument,
    // but we want a clear bad-request message.
    if (str[0] == '-') {
        throw BadRequestException("Invalid " + paramName + " parameter: must be a non-negative integer");
    }
    std::size_t pos = 0;
    unsigned long result = 0;
    try {
        result = std::stoul(str, &pos);
    } catch (const std::invalid_argument&) {
        throw BadRequestException("Invalid " + paramName + " parameter: not a valid integer");
    } catch (const std::out_of_range&) {
        throw BadRequestException("Invalid " + paramName + " parameter: value out of range");
    }
    // Reject trailing non-numeric characters (e.g. "3.14", "10abc")
    if (pos != str.size()) {
        throw BadRequestException("Invalid " + paramName + " parameter: not a valid integer");
    }
    return static_cast<size_t>(result);
}

// static
std::string HttpMetadataHandler::validateQueryParam(const std::string& name, const std::string& context) {
    if (name.empty()) {
        return context + " must not be empty";
    }
    for (char c : name) {
        if (c == '\0')
            return context + " must not contain null bytes";
        if (static_cast<unsigned char>(c) < 32)
            return context + " must not contain control characters";
    }
    return "";  // Valid
}

HttpMetadataHandler::HttpMetadataHandler(seastar::sharded<Engine>* _engineSharded) : engineSharded(_engineSharded) {
    timestar::http_log.info("HttpMetadataHandler initialized");
}

void HttpMetadataHandler::registerRoutes(seastar::httpd::routes& r, std::string_view authToken) {
    // addJsonRoute applies timestar::http::wrapWithAuth per route.
    using op = seastar::httpd::operation_type;

    timestar::http::addJsonRoute(
        r, op::GET, "/measurements", authToken,
        [this](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply>)
            -> seastar::future<std::unique_ptr<seastar::http::reply>> { return handleMeasurements(std::move(req)); });

    timestar::http::addJsonRoute(
        r, op::GET, "/tags", authToken,
        [this](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply>)
            -> seastar::future<std::unique_ptr<seastar::http::reply>> { return handleTags(std::move(req)); });

    timestar::http::addJsonRoute(
        r, op::GET, "/fields", authToken,
        [this](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply>)
            -> seastar::future<std::unique_ptr<seastar::http::reply>> { return handleFields(std::move(req)); });

    timestar::http::addJsonRoute(
        r, op::GET, "/cardinality", authToken,
        [this](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply>)
            -> seastar::future<std::unique_ptr<seastar::http::reply>> { return handleCardinality(std::move(req)); });

    timestar::http::addJsonRoute(
        r, op::GET, "/series", authToken,
        [this](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply>)
            -> seastar::future<std::unique_ptr<seastar::http::reply>> { return handleSeriesLookup(std::move(req)); });

    timestar::http_log.info("Registered metadata endpoints: /measurements, /tags, /fields, /cardinality, /series{}",
                            authToken.empty() ? "" : " (auth required)");
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpMetadataHandler::handleMeasurements(
    std::unique_ptr<seastar::http::request> req) {
    auto rep = std::make_unique<seastar::http::reply>();
    auto resFmt = timestar::http::responseFormat(*req);

    try {
        timestar::http_log.debug("Processing /measurements request");

        // Schema caches are populated on all shards via broadcast, so query locally
        auto measurements = co_await engineSharded->local().getAllMeasurements();

        // Apply filters if specified
        // Note: prefix is only used for starts_with comparison, so null bytes or other
        // special characters are harmless — they simply won't match any measurement name.
        std::string prefix = req->get_query_param("prefix");
        if (!prefix.empty()) {
            std::erase_if(measurements, [&prefix](const std::string& m) { return !m.starts_with(prefix); });
        }

        // Apply pagination - validate parameters and return 400 on invalid input
        size_t offset = 0;
        size_t limit = std::numeric_limits<size_t>::max();

        try {
            offset = parsePaginationParam(req->get_query_param("offset"), "offset", 0);
            limit = parsePaginationParam(req->get_query_param("limit"), "limit", std::numeric_limits<size_t>::max());
        } catch (const BadRequestException& e) {
            timestar::http::respond(*rep, seastar::http::reply::status_type::bad_request, resFmt,
                                    timestar::http::isProtobuf(resFmt)
                                        ? timestar::proto::formatErrorResponse(e.what(), "INVALID_PARAMETER")
                                        : createErrorResponse("INVALID_PARAMETER", e.what()));
            co_return rep;
        }

        size_t totalCount = measurements.size();

        if (offset >= measurements.size()) {
            measurements.clear();
        } else if (offset > 0 || limit < measurements.size() - offset) {
            size_t end = (limit > measurements.size() - offset) ? measurements.size() : offset + limit;
            measurements = std::vector<std::string>(std::make_move_iterator(measurements.begin() + offset),
                                                    std::make_move_iterator(measurements.begin() + end));
        }
        // else: offset==0 && limit >= size — no copy needed

        // Format response
        rep->set_status(seastar::http::reply::status_type::ok);
        if (timestar::http::isProtobuf(resFmt)) {
            rep->_content = timestar::proto::formatMeasurementsResponse(measurements, totalCount);
        } else {
            rep->_content = formatMeasurementsResponse(measurements, totalCount);
        }

        timestar::http_log.debug("Returning {} measurements", measurements.size());

    } catch (const std::exception& e) {
        timestar::http_log.error("Error processing /measurements: {}", e.what());
        timestar::http::respond(*rep, seastar::http::reply::status_type::internal_server_error, resFmt,
                                timestar::http::isProtobuf(resFmt)
                                    ? timestar::proto::formatErrorResponse("Internal server error", "INTERNAL_ERROR")
                                    : createErrorResponse("INTERNAL_ERROR", "Internal server error"));
        co_return rep;
    }

    timestar::http::setContentType(*rep, resFmt);
    co_return rep;
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpMetadataHandler::handleTags(
    std::unique_ptr<seastar::http::request> req) {
    auto rep = std::make_unique<seastar::http::reply>();
    auto resFmt = timestar::http::responseFormat(*req);

    try {
        std::string measurement = req->get_query_param("measurement");
        if (measurement.empty()) {
            timestar::http::respond(
                *rep, seastar::http::reply::status_type::bad_request, resFmt,
                timestar::http::isProtobuf(resFmt)
                    ? timestar::proto::formatErrorResponse("measurement parameter is required", "MISSING_PARAMETER")
                    : createErrorResponse("MISSING_PARAMETER", "measurement parameter is required"));
            co_return rep;
        }

        {
            auto err = validateQueryParam(measurement, "Measurement name");
            if (!err.empty()) {
                timestar::http::respond(*rep, seastar::http::reply::status_type::bad_request, resFmt,
                                        timestar::http::isProtobuf(resFmt)
                                            ? timestar::proto::formatErrorResponse(err, "INVALID_PARAMETER")
                                            : createErrorResponse("INVALID_PARAMETER", err));
                co_return rep;
            }
        }

        std::string specificTag = req->get_query_param("tag");
        if (!specificTag.empty()) {
            auto err = validateQueryParam(specificTag, "Tag name");
            if (!err.empty()) {
                timestar::http::respond(*rep, seastar::http::reply::status_type::bad_request, resFmt,
                                        timestar::http::isProtobuf(resFmt)
                                            ? timestar::proto::formatErrorResponse(err, "INVALID_PARAMETER")
                                            : createErrorResponse("INVALID_PARAMETER", err));
                co_return rep;
            }
        }

        timestar::http_log.debug("Processing /tags request for measurement: {}, tag: {}", measurement,
                                 specificTag.empty() ? "all" : specificTag);

        // Schema caches are populated on all shards via broadcast, so query locally
        std::unordered_map<std::string, std::set<std::string>> allTagsResults;
        auto& localEngine = engineSharded->local();

        if (specificTag.empty()) {
            // Get all tags for measurement, then fetch values in parallel
            auto tagKeys = co_await localEngine.getMeasurementTags(measurement);
            std::vector<seastar::future<std::set<std::string>>> futures;
            futures.reserve(tagKeys.size());
            std::vector<std::string> keyVec(tagKeys.begin(), tagKeys.end());
            for (const auto& tagKey : keyVec) {
                futures.push_back(localEngine.getTagValues(measurement, tagKey));
            }
            auto allValues = co_await seastar::when_all_succeed(futures.begin(), futures.end());
            for (size_t i = 0; i < keyVec.size(); ++i) {
                allTagsResults[keyVec[i]] = std::move(allValues[i]);
            }
        } else {
            // Get values for specific tag
            auto tagValues = co_await localEngine.getTagValues(measurement, specificTag);
            allTagsResults[specificTag] = tagValues;
        }

        // Convert sets to vectors for response formatting
        std::unordered_map<std::string, std::vector<std::string>> tagsResult;
        for (const auto& [tagKey, tagValues] : allTagsResults) {
            // tagValues is a std::set — already sorted, no need to re-sort
            tagsResult[tagKey] = std::vector<std::string>(tagValues.begin(), tagValues.end());
        }

        rep->set_status(seastar::http::reply::status_type::ok);
        if (timestar::http::isProtobuf(resFmt)) {
            rep->_content = timestar::proto::formatTagsResponse(measurement, tagsResult);
        } else {
            rep->_content = formatTagsResponse(measurement, tagsResult, specificTag);
        }

        timestar::http_log.debug("Returning tags for measurement: {}", measurement);

    } catch (const std::exception& e) {
        timestar::http_log.error("Error processing /tags: {}", e.what());
        timestar::http::respond(*rep, seastar::http::reply::status_type::internal_server_error, resFmt,
                                timestar::http::isProtobuf(resFmt)
                                    ? timestar::proto::formatErrorResponse("Internal server error", "INTERNAL_ERROR")
                                    : createErrorResponse("INTERNAL_ERROR", "Internal server error"));
        co_return rep;
    }

    timestar::http::setContentType(*rep, resFmt);
    co_return rep;
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpMetadataHandler::handleFields(
    std::unique_ptr<seastar::http::request> req) {
    auto rep = std::make_unique<seastar::http::reply>();
    auto resFmt = timestar::http::responseFormat(*req);

    try {
        std::string measurement = req->get_query_param("measurement");
        if (measurement.empty()) {
            timestar::http::respond(
                *rep, seastar::http::reply::status_type::bad_request, resFmt,
                timestar::http::isProtobuf(resFmt)
                    ? timestar::proto::formatErrorResponse("measurement parameter is required", "MISSING_PARAMETER")
                    : createErrorResponse("MISSING_PARAMETER", "measurement parameter is required"));
            co_return rep;
        }

        {
            auto err = validateQueryParam(measurement, "Measurement name");
            if (!err.empty()) {
                timestar::http::respond(*rep, seastar::http::reply::status_type::bad_request, resFmt,
                                        timestar::http::isProtobuf(resFmt)
                                            ? timestar::proto::formatErrorResponse(err, "INVALID_PARAMETER")
                                            : createErrorResponse("INVALID_PARAMETER", err));
                co_return rep;
            }
        }

        timestar::http_log.debug("Processing /fields request for measurement: {}", measurement);

        // TODO: tagFilters are parsed but not yet applied to field discovery.
        // The "filtered_by" response field shows the parsed filters for API transparency.
        // Implementing tag-based field filtering requires cross-referencing the index.
        // Parse optional tags parameter for filtering
        std::string tagsParam = req->get_query_param("tags");
        std::unordered_map<std::string, std::string> tagFilters;
        if (!tagsParam.empty()) {
            // Parse tags in format "key1:value1,key2:value2"
            // Use only the first colon as delimiter so values can contain colons
            // (e.g., "host:port:8080" -> key="host", value="port:8080")
            size_t pos = 0;
            while (pos < tagsParam.length()) {
                size_t colonPos = tagsParam.find(':', pos);
                if (colonPos == std::string::npos) {
                    // Malformed segment: no colon found. Return 400 so the
                    // caller knows their input is wrong rather than silently
                    // dropping this and all subsequent segments.
                    size_t endPos = tagsParam.find(',', pos);
                    if (endPos == std::string::npos)
                        endPos = tagsParam.length();
                    std::string badSegment = tagsParam.substr(pos, endPos - pos);
                    auto errMsg = "Malformed tag filter segment '" + badSegment + "': expected 'key:value' format";
                    timestar::http::respond(*rep, seastar::http::reply::status_type::bad_request, resFmt,
                                            timestar::http::isProtobuf(resFmt)
                                                ? timestar::proto::formatErrorResponse(errMsg, "INVALID_PARAMETER")
                                                : createErrorResponse("INVALID_PARAMETER", errMsg));
                    co_return rep;
                }

                std::string key = tagsParam.substr(pos, colonPos - pos);

                // Find the next comma after the key:value pair.
                // We need to find the next comma that is NOT part of the value.
                // Since we split on the first colon, the value runs until the next
                // top-level comma or end of string.
                size_t commaPos = tagsParam.find(',', colonPos + 1);
                if (commaPos == std::string::npos)
                    commaPos = tagsParam.length();

                std::string value = tagsParam.substr(colonPos + 1, commaPos - colonPos - 1);
                tagFilters[key] = value;

                pos = commaPos + 1;
            }
        }

        // Schema caches are populated on all shards via broadcast, so query locally
        auto& localEngine = engineSharded->local();
        auto allFields = co_await localEngine.getMeasurementFields(measurement);

        std::unordered_map<std::string, std::string> fieldsWithTypes;
        std::vector<std::string> fieldVec(allFields.begin(), allFields.end());
        std::vector<seastar::future<std::string>> typeFutures;
        typeFutures.reserve(fieldVec.size());
        for (const auto& field : fieldVec) {
            typeFutures.push_back(localEngine.getIndex().getFieldType(measurement, field));
        }
        auto allTypes = co_await seastar::when_all_succeed(typeFutures.begin(), typeFutures.end());
        for (size_t i = 0; i < fieldVec.size(); ++i) {
            fieldsWithTypes[fieldVec[i]] = allTypes[i].empty() ? "float" : std::move(allTypes[i]);
        }

        rep->set_status(seastar::http::reply::status_type::ok);
        if (timestar::http::isProtobuf(resFmt)) {
            rep->_content = timestar::proto::formatFieldsResponse(measurement, fieldsWithTypes);
        } else {
            rep->_content = formatFieldsResponse(measurement, fieldsWithTypes, tagFilters);
        }

        timestar::http_log.debug("Returning {} fields for measurement: {}", allFields.size(), measurement);

    } catch (const std::exception& e) {
        timestar::http_log.error("Error processing /fields: {}", e.what());
        timestar::http::respond(*rep, seastar::http::reply::status_type::internal_server_error, resFmt,
                                timestar::http::isProtobuf(resFmt)
                                    ? timestar::proto::formatErrorResponse("Internal server error", "INTERNAL_ERROR")
                                    : createErrorResponse("INTERNAL_ERROR", "Internal server error"));
        co_return rep;
    }

    timestar::http::setContentType(*rep, resFmt);
    co_return rep;
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpMetadataHandler::handleSeriesLookup(
    std::unique_ptr<seastar::http::request> req) {
    auto rep = std::make_unique<seastar::http::reply>();
    auto resFmt = timestar::http::responseFormat(*req);

    try {
        std::string idHex = req->get_query_param("id");
        if (idHex.empty()) {
            static constexpr const char* kMsg = "Query parameter 'id' is required (32 hex characters)";
            timestar::http::respond(*rep, seastar::http::reply::status_type::bad_request, resFmt,
                                    timestar::http::isProtobuf(resFmt)
                                        ? timestar::proto::formatErrorResponse(kMsg, "MISSING_PARAMETER")
                                        : createErrorResponse("MISSING_PARAMETER", kMsg));
            co_return rep;
        }

        SeriesId128 seriesId;
        try {
            seriesId = SeriesId128::fromHex(idHex);
        } catch (const std::exception& e) {
            std::string msg = std::string("Invalid series id: ") + e.what();
            timestar::http::respond(*rep, seastar::http::reply::status_type::bad_request, resFmt,
                                    timestar::http::isProtobuf(resFmt)
                                        ? timestar::proto::formatErrorResponse(msg, "INVALID_PARAMETER")
                                        : createErrorResponse("INVALID_PARAMETER", msg));
            co_return rep;
        }

        // Series metadata and the type binding are shard-local, owned by the
        // shard the id routes to. No fan-out needed.
        unsigned owner = timestar::routeToCore(seriesId);
        auto found = co_await engineSharded->invoke_on(
            owner,
            [seriesId](Engine& engine) -> seastar::future<std::pair<std::optional<SeriesMetadata>, std::string>> {
                auto meta = co_await engine.getIndex().getSeriesMetadata(seriesId);
                auto bound = co_await engine.getIndex().getSeriesValueType(seriesId);
                co_return std::make_pair(std::move(meta),
                                         bound.has_value() ? std::string(timestar::valueTypeName(*bound)) : "");
            });

        if (!found.first.has_value()) {
            static constexpr const char* kMsg = "No series with that id on the owning shard";
            timestar::http::respond(*rep, seastar::http::reply::status_type::not_found, resFmt,
                                    timestar::http::isProtobuf(resFmt)
                                        ? timestar::proto::formatErrorResponse(kMsg, "NOT_FOUND")
                                        : createErrorResponse("NOT_FOUND", kMsg));
            co_return rep;
        }

        SeriesLookupResponse response;
        response.series_id = seriesId.toHex();
        response.measurement = found.first->measurement;
        response.field = found.first->field;
        response.tags = std::map<std::string, std::string>(found.first->tags.begin(), found.first->tags.end());
        response.value_type = found.second;
        response.shard = owner;

        std::string buffer;
        (void)glz::write_json(response, buffer);
        rep->set_status(seastar::http::reply::status_type::ok);
        rep->_content = std::move(buffer);
    } catch (const std::exception& e) {
        timestar::http_log.error("Error in /series: {}", e.what());
        timestar::http::respond(*rep, seastar::http::reply::status_type::internal_server_error, resFmt,
                                timestar::http::isProtobuf(resFmt)
                                    ? timestar::proto::formatErrorResponse(e.what(), "INTERNAL_ERROR")
                                    : createErrorResponse("INTERNAL_ERROR", e.what()));
    }

    timestar::http::setContentType(*rep, resFmt);
    co_return rep;
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpMetadataHandler::handleCardinality(
    std::unique_ptr<seastar::http::request> req) {
    auto rep = std::make_unique<seastar::http::reply>();
    auto resFmt = timestar::http::responseFormat(*req);

    try {
        std::string measurement = req->get_query_param("measurement");
        if (measurement.empty()) {
            timestar::http::respond(
                *rep, seastar::http::reply::status_type::bad_request, resFmt,
                timestar::http::isProtobuf(resFmt)
                    ? timestar::proto::formatErrorResponse("measurement parameter is required", "MISSING_PARAMETER")
                    : createErrorResponse("MISSING_PARAMETER", "measurement parameter is required"));
            co_return rep;
        }

        {
            auto err = validateQueryParam(measurement, "Measurement name");
            if (!err.empty()) {
                timestar::http::respond(*rep, seastar::http::reply::status_type::bad_request, resFmt,
                                        timestar::http::isProtobuf(resFmt)
                                            ? timestar::proto::formatErrorResponse(err, "INVALID_PARAMETER")
                                            : createErrorResponse("INVALID_PARAMETER", err));
                co_return rep;
            }
        }

        std::string tagKey = req->get_query_param("tag_key");
        std::string tagValue = req->get_query_param("tag_value");

        if (!tagKey.empty()) {
            auto err = validateQueryParam(tagKey, "Tag key");
            if (!err.empty()) {
                rep->set_status(seastar::http::reply::status_type::bad_request);
                if (timestar::http::isProtobuf(resFmt)) {
                    rep->_content = timestar::proto::formatErrorResponse(err, "INVALID_PARAMETER");
                } else {
                    rep->_content = createErrorResponse("INVALID_PARAMETER", err);
                }
                timestar::http::setContentType(*rep, resFmt);
                co_return rep;
            }
        }
        if (!tagValue.empty()) {
            auto err = validateQueryParam(tagValue, "Tag value");
            if (!err.empty()) {
                rep->set_status(seastar::http::reply::status_type::bad_request);
                if (timestar::http::isProtobuf(resFmt)) {
                    rep->_content = timestar::proto::formatErrorResponse(err, "INVALID_PARAMETER");
                } else {
                    rep->_content = createErrorResponse("INVALID_PARAMETER", err);
                }
                timestar::http::setContentType(*rep, resFmt);
                co_return rep;
            }
        }

        timestar::http_log.debug("Processing /cardinality request for measurement: {}", measurement);

        // Validate: both tag_key and tag_value must be provided together
        if (tagKey.empty() != tagValue.empty()) {
            constexpr const char* kMsg = "Both tag_key and tag_value must be provided together";
            timestar::http::respond(*rep, seastar::http::reply::status_type::bad_request, resFmt,
                                    timestar::http::isProtobuf(resFmt)
                                        ? timestar::proto::formatErrorResponse(kMsg, "INVALID_PARAMETER")
                                        : createErrorResponse("INVALID_PARAMETER", kMsg));
            co_return rep;
        }

        // Scatter-gather: sum estimates across all shards. Per-shard HLLs track
        // different LocalIds, so summing is correct for the cross-shard total.

        if (!tagKey.empty() && !tagValue.empty()) {
            // Specific tag combination cardinality
            double total = co_await timestar::cluster::scatterAndSum(
                *engineSharded, [measurement, tagKey, tagValue](Engine& engine) {
                    return engine.getIndex().estimateTagCardinality(measurement, tagKey, tagValue);
                });

            std::unordered_map<std::string, double> tagCard;
            tagCard[tagKey + ":" + tagValue] = total;

            rep->set_status(seastar::http::reply::status_type::ok);
            if (timestar::http::isProtobuf(resFmt)) {
                rep->_content = timestar::proto::formatCardinalityResponse(measurement, total, tagCard);
            } else {
                rep->_content = formatCardinalityResponse(measurement, total, tagCard);
            }
        } else {
            // Measurement-level cardinality plus per-tag-key cardinalities
            double totalEstimate =
                co_await timestar::cluster::scatterAndSum(*engineSharded, [measurement](Engine& engine) {
                    return engine.getIndex().estimateMeasurementCardinality(measurement);
                });

            // Get tag keys from local schema cache, then estimate per-tag-key cardinality
            auto& localEngine = engineSharded->local();
            auto tagKeys = co_await localEngine.getMeasurementTags(measurement);

            // Fetch all tag-key cardinalities in parallel (not sequentially)
            std::unordered_map<std::string, double> tagCardinalities;
            std::vector<seastar::future<std::set<std::string>>> tagFutures;
            tagFutures.reserve(tagKeys.size());
            std::vector<std::string> tagKeysCopy(tagKeys.begin(), tagKeys.end());
            for (const auto& tk : tagKeysCopy) {
                tagFutures.push_back(localEngine.getTagValues(measurement, tk));
            }
            auto tagResults = co_await seastar::when_all_succeed(tagFutures.begin(), tagFutures.end());
            for (size_t i = 0; i < tagKeysCopy.size(); ++i) {
                tagCardinalities[tagKeysCopy[i]] = static_cast<double>(tagResults[i].size());
            }

            rep->set_status(seastar::http::reply::status_type::ok);
            if (timestar::http::isProtobuf(resFmt)) {
                rep->_content =
                    timestar::proto::formatCardinalityResponse(measurement, totalEstimate, tagCardinalities);
            } else {
                rep->_content = formatCardinalityResponse(measurement, totalEstimate, tagCardinalities);
            }
        }

        timestar::http_log.debug("Returning cardinality for measurement: {}", measurement);

    } catch (const std::exception& e) {
        timestar::http_log.error("Error processing /cardinality: {}", e.what());
        timestar::http::respond(*rep, seastar::http::reply::status_type::internal_server_error, resFmt,
                                timestar::http::isProtobuf(resFmt)
                                    ? timestar::proto::formatErrorResponse("Internal server error", "INTERNAL_ERROR")
                                    : createErrorResponse("INTERNAL_ERROR", "Internal server error"));
        co_return rep;
    }

    timestar::http::setContentType(*rep, resFmt);
    co_return rep;
}

std::string HttpMetadataHandler::createErrorResponse(const std::string& code, const std::string& message) {
    // Canonical flat error shape shared by all handlers (see http_error.hpp).
    // Previously this handler nested {"error":{"code","message"}}.
    return timestar::http::jsonError(message, code);
}

std::string HttpMetadataHandler::formatMeasurementsResponse(const std::vector<std::string>& measurements,
                                                            size_t total) {
    MeasurementsResponse response;
    response.measurements = measurements;
    response.total = total;

    std::string buffer;
    (void)glz::write_json(response, buffer);
    return buffer;
}

std::string HttpMetadataHandler::formatTagsResponse(
    const std::string& measurement, const std::unordered_map<std::string, std::vector<std::string>>& tags,
    const std::string& specificTag) {
    if (!specificTag.empty()) {
        // Return specific tag values
        TagValuesResponse response;
        response.measurement = measurement;
        response.tag = specificTag;
        if (auto it = tags.find(specificTag); it != tags.end()) {
            response.values = it->second;
        }

        std::string buffer;
        (void)glz::write_json(response, buffer);
        return buffer;
    } else {
        // Return all tags
        TagsResponse response;
        response.measurement = measurement;
        response.tags = tags;

        std::string buffer;
        (void)glz::write_json(response, buffer);
        return buffer;
    }
}

std::string HttpMetadataHandler::formatFieldsResponse(const std::string& measurement,
                                                      const std::unordered_map<std::string, std::string>& fields,
                                                      const std::unordered_map<std::string, std::string>& tagFilters) {
    FieldsResponse response;
    response.measurement = measurement;

    for (const auto& [fieldName, fieldType] : fields) {
        response.fields[fieldName].name = fieldName;
        response.fields[fieldName].type = fieldType;
    }

    if (!tagFilters.empty()) {
        response.filtered_by = tagFilters;
    }

    std::string buffer;
    (void)glz::write_json(response, buffer);
    return buffer;
}

std::string HttpMetadataHandler::formatCardinalityResponse(
    const std::string& measurement, double estimatedSeriesCount,
    const std::unordered_map<std::string, double>& tagCardinalities) {
    CardinalityResponse response;
    response.measurement = measurement;
    response.estimated_series_count = estimatedSeriesCount;
    response.tag_cardinalities = tagCardinalities;

    std::string buffer;
    (void)glz::write_json(response, buffer);
    return buffer;
}

}  // namespace timestar::http
