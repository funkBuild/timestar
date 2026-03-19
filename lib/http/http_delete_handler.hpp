#pragma once

#include "engine.hpp"
#include "series_id.hpp"

#include <glaze/glaze.hpp>

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

// Glaze-compatible structure for JSON parsing of delete requests.
// Defined here (not in the .cpp) to avoid ODR violations across translation units.
struct GlazeDeleteRequest {
    // For series key format
    std::optional<std::string> series;

    // For structured format
    std::optional<std::string> measurement;
    std::optional<std::map<std::string, std::string>> tags;
    std::optional<std::string> field;
    std::optional<std::vector<std::string>> fields;

    // Time range (optional - defaults to all time)
    std::optional<uint64_t> startTime;
    std::optional<uint64_t> endTime;
};

template <>
struct glz::meta<GlazeDeleteRequest> {
    using T = GlazeDeleteRequest;
    static constexpr auto value =
        object("series", &T::series, "measurement", &T::measurement, "tags", &T::tags, "field", &T::field, "fields",
               &T::fields, "startTime", &T::startTime, "endTime", &T::endTime);
};

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>

/*
 * JSON Schema for Delete Requests:
 *
 * Delete by series key:
 * {
 *   "series": "temperature,location=us-west.value",
 *   "startTime": 1465839830100400200,  // nanoseconds since epoch
 *   "endTime": 1465839930100400200     // nanoseconds since epoch
 * }
 *
 * Delete by structured series (exact match):
 * {
 *   "measurement": "temperature",
 *   "tags": {
 *     "location": "us-west",
 *     "host": "server-01"
 *   },
 *   "field": "value",
 *   "startTime": 1465839830100400200,
 *   "endTime": 1465839930100400200
 * }
 *
 * Flexible delete by pattern:
 * {
 *   "measurement": "temperature",
 *   "tags": {                          // Optional: omit to match all tags
 *     "location": "us-west"             // Only series with this tag
 *   },
 *   "fields": ["value", "humidity"],   // Optional: omit to match all fields
 *   "startTime": 1465839830100400200,
 *   "endTime": 1465839930100400200
 * }
 *
 * Delete all data for a measurement:
 * {
 *   "measurement": "temperature",
 *   "startTime": 0,
 *   "endTime": 9223372036854775807     // Max int64
 * }
 *
 * Batch deletes:
 * {
 *   "deletes": [
 *     {
 *       "measurement": "temperature",
 *       "tags": { "location": "us-west" },
 *       "fields": ["value"],
 *       "startTime": 1465839830100400200,
 *       "endTime": 1465839930100400200
 *     }
 *   ]
 * }
 */

class HttpDeleteHandler {
private:
    seastar::sharded<Engine>* engineSharded;

public:
    struct DeleteRequest {
        std::string seriesKey;
        std::string measurement;
        std::map<std::string, std::string> tags;
        std::string field;                // For single field deletion
        std::vector<std::string> fields;  // For multiple field deletion
        uint64_t startTime;
        uint64_t endTime;
        bool isStructured;  // true if using measurement/tags/field format
        bool isPattern;     // true if using pattern-based deletion
    };

    DeleteRequest parseDeleteRequest(const GlazeDeleteRequest& glazeReq);
    std::string createErrorResponse(const std::string& error);
    std::string createSuccessResponse(uint64_t deletedCount, uint64_t totalRequests);

    HttpDeleteHandler(seastar::sharded<Engine>* _engineSharded) : engineSharded(_engineSharded) {}

    seastar::future<std::unique_ptr<seastar::http::reply>> handleDelete(std::unique_ptr<seastar::http::request> req);

    void registerRoutes(seastar::httpd::routes& r, std::string_view authToken = "");
};
