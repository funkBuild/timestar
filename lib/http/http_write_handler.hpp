#pragma once

#include "engine.hpp"
#include "series_id.hpp"
#include "timestar_config.hpp"
#include "timestar_value.hpp"
#include "wal_file_manager.hpp"

#include <glaze/glaze.hpp>

#include <tsl/robin_map.h>

#include <chrono>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>
#include <string>
#include <variant>

/*
 * JSON Schema for Write Requests:
 *
 * Single point:
 * {
 *   "measurement": "temperature",
 *   "tags": {
 *     "location": "us-midwest",
 *     "host": "server-01"
 *   },
 *   "fields": {
 *     "value": 82.0,
 *     "humidity": 65.0
 *   },
 *   "timestamp": 1465839830100400200  // nanoseconds since epoch
 * }
 *
 * Multiple points (array format):
 * {
 *   "measurement": "temperature",
 *   "tags": {
 *     "location": "us-midwest",
 *     "host": "server-01"
 *   },
 *   "fields": {
 *     "value": [82.0, 83.0, 82.5, 83.5],      // Array of values
 *     "humidity": [65.0, 64.5, 65.5, 64.0]    // Must be same length
 *   },
 *   "timestamps": [1000000000, 1000001000, 1000002000, 1000003000]  // Array of timestamps
 * }
 *
 * Or batch writes:
 * {
 *   "writes": [
 *     {
 *       "measurement": "temperature",
 *       "tags": { "location": "us-midwest", "host": "server-01" },
 *       "fields": { "value": 82.0 },
 *       "timestamp": 1465839830100400200
 *     },
 *     {
 *       "measurement": "temperature",
 *       "tags": { "location": "us-west", "host": "server-02" },
 *       "fields": { "value": [75.0, 76.0] },
 *       "timestamps": [1465839830100400200, 1465839831100400200]
 *     }
 *   ]
 * }
 */

// Use glz::generic_u64 for JSON parsing to preserve uint64_t timestamp precision.
// glz::json_t (glz::generic) uses num_mode::f64, which parses all numbers as double,
// losing precision for nanosecond timestamps (double has only 53 bits of mantissa,
// while ns timestamps require ~61 bits). generic_u64 stores integers as uint64_t/int64_t.
using json_value_t = glz::generic_u64;

class HttpWriteHandler {
public:
    // Security limit to prevent DoS attacks via large request bodies
    static size_t maxWriteBodySize() { return timestar::config().http.max_write_body_size; }

    // Field value variant for flexible JSON parsing (public for validation API)
    using FieldValue = std::variant<double, bool, std::string, int64_t>;

private:
    seastar::sharded<Engine>* engineSharded;

    struct WritePoint {
        std::string measurement;
        std::map<std::string, std::string> tags;
        std::map<std::string, FieldValue> fields;
        uint64_t timestamp;
    };

    struct FieldArrays {
        std::vector<double> doubles;
        std::vector<uint8_t> bools;
        std::vector<std::string> strings;
        std::vector<int64_t> integers;
        enum Type { DOUBLE, BOOL, STRING, INTEGER } type;
    };

    struct MultiWritePoint {
        std::string measurement;
        std::map<std::string, std::string> tags;
        std::map<std::string, FieldArrays> fields;  // Field name -> array of values
        std::vector<uint64_t> timestamps;           // Array of timestamps
    };

    // Structure for coalescing multiple individual writes into array writes
    struct CoalesceCandidate {
        std::string seriesKey;  // measurement + tags + field for grouping
        std::string groupKey;   // measurement + tags (no field) for coalesce grouping
        std::string measurement;
        // Shared (refcounted) tag map to avoid redundant copies when multiple
        // fields from the same write point each create a CoalesceCandidate.
        // Uses lw_shared_ptr (non-atomic refcount) because all candidates
        // live on the HTTP handler shard -- no cross-shard sharing needed.
        seastar::lw_shared_ptr<const std::map<std::string, std::string>> sharedTags;
        std::string fieldName;
        TSMValueType valueType;
        std::vector<uint64_t> timestamps;
        uint64_t timestampHashSum = 0;  // commutative sum of all timestamps
        uint64_t timestampHashXor = 0;  // commutative XOR of all timestamps

        // Value storage by type (only one will be used based on valueType)
        std::vector<double> doubleValues;
        std::vector<uint8_t> boolValues;
        std::vector<std::string> stringValues;
        std::vector<int64_t> integerValues;

        // Helper to add a value with timestamp
        void addValue(uint64_t timestamp, double value) {
            timestamps.push_back(timestamp);
            timestampHashSum += timestamp;
            timestampHashXor ^= timestamp;
            doubleValues.push_back(value);
        }

        void addValue(uint64_t timestamp, bool value) {
            timestamps.push_back(timestamp);
            timestampHashSum += timestamp;
            timestampHashXor ^= timestamp;
            boolValues.push_back(value);
        }

        void addValue(uint64_t timestamp, const std::string& value) {
            timestamps.push_back(timestamp);
            timestampHashSum += timestamp;
            timestampHashXor ^= timestamp;
            stringValues.push_back(value);
        }

        void addValue(uint64_t timestamp, std::string&& value) {
            timestamps.push_back(timestamp);
            timestampHashSum += timestamp;
            timestampHashXor ^= timestamp;
            stringValues.push_back(std::move(value));
        }

        void addValue(uint64_t timestamp, int64_t value) {
            timestamps.push_back(timestamp);
            timestampHashSum += timestamp;
            timestampHashXor ^= timestamp;
            integerValues.push_back(value);
        }
    };

    // Structure to collect timing information across all operations
    struct AggregatedTimingInfo {
        std::chrono::microseconds totalCompressionTime = std::chrono::microseconds(0);
        std::chrono::microseconds totalWalWriteTime = std::chrono::microseconds(0);
        int totalWalWriteCount = 0;

        void aggregate(const WALTimingInfo& walTiming) {
            totalCompressionTime += walTiming.compressionTime;
            totalWalWriteTime += walTiming.walWriteTime;
            totalWalWriteCount += walTiming.walWriteCount;
        }

        void aggregate(const AggregatedTimingInfo& other) {
            totalCompressionTime += other.totalCompressionTime;
            totalWalWriteTime += other.totalWalWriteTime;
            totalWalWriteCount += other.totalWalWriteCount;
        }
    };

    // Helper struct for metadata operations (deduplication across batch).
    // Aliases MetadataOp from index_backend.hpp for batch indexing compatibility.
    using MetaOp = MetadataOp;

    // Parse a single write point from JSON string.
    // defaultTimestampNs is used when the point has no explicit timestamp,
    // avoiding a redundant now() call when the caller already captured one.
    WritePoint parseWritePoint(const std::string& json, uint64_t defaultTimestampNs);

    // Parse a single write point from an already-parsed JSON object.
    // defaultTimestampNs is used when the point has no explicit timestamp.
    WritePoint parseWritePoint(const json_value_t& doc, uint64_t defaultTimestampNs);

    // Parse a write point that may contain arrays.
    // defaultTimestampNs is used when the point has no explicit timestamp,
    // so the caller can capture now() once for an entire batch.
    MultiWritePoint parseMultiWritePoint(const json_value_t& point, uint64_t defaultTimestampNs);

    // Process a single write point - determine type and insert
    seastar::future<> processWritePoint(const WritePoint& point);

    // Result of processMultiWritePoint: timing info plus any metadata ops
    // that were collected locally (no shared mutable state across coroutines).
    struct WriteResult {
        AggregatedTimingInfo timing;
        std::vector<MetaOp> metaOps;
    };

    // Process a multi-point write with arrays.
    // Returns timing info and locally-collected metadata ops.
    // Takes point by non-const ref so the last field's tags/timestamps can be moved.
    seastar::future<WriteResult> processMultiWritePoint(MultiWritePoint& point);

    // Build a MultiWritePoint from a FastDoubleWritePoint (fast-path, all fields are doubles).
    // Returns true on success, false on validation failure.
    static bool buildMWPFromFastPath(struct FastDoubleWritePoint& fwp, uint64_t defaultTimestampNs,
                                     MultiWritePoint& mwp);

    // Validate that all field arrays have the same length as timestamps
    bool validateArraySizes(const MultiWritePoint& point, std::string& error);

    // Coalesce multiple individual writes into efficient array writes.
    // defaultTimestampNs is the pre-computed current time used for any write
    // that lacks an explicit timestamp, avoiding per-write now() calls.
    std::vector<MultiWritePoint> coalesceWrites(const json_value_t::array_t& writes_array, uint64_t defaultTimestampNs);

    // Create error response JSON
    std::string createErrorResponse(const std::string& error);

    // Create success response JSON
    std::string createSuccessResponse(int64_t pointsWritten);

    // Create partial failure response JSON (some writes in a batch failed)
    std::string createPartialFailureResponse(int64_t pointsWritten, int64_t failedWrites,
                                             const std::vector<std::string>& errors);

public:
    HttpWriteHandler(seastar::sharded<Engine>* _engineSharded);

    // Main handler for write requests
    seastar::future<std::unique_ptr<seastar::http::reply>> handleWrite(std::unique_ptr<seastar::http::request> req);

    // Register routes with HTTP server
    void registerRoutes(seastar::httpd::routes& r, std::string_view authToken = "");

    // Validate that a name (measurement, tag key, field name) does not contain
    // reserved separator characters that would corrupt key encoding.
    // Returns empty string if valid, or an error description if invalid.
    static std::string validateName(const std::string& name, const std::string& context);

    // Validate a tag value. Same as validateName but allows spaces,
    // since spaces don't participate in tag value key encoding.
    // Returns empty string if valid, or an error description if invalid.
    static std::string validateTagValue(const std::string& value, const std::string& context);

    // Validate all names in a write point (measurement, tags, fields).
    // Throws std::runtime_error if any name is invalid.
    static void validateWritePointNames(const std::string& measurement, const std::map<std::string, std::string>& tags,
                                        const std::map<std::string, FieldValue>& fields);

    // Parse and validate a single write point from JSON string (for testing without Seastar).
    // Throws std::runtime_error if JSON is invalid or names contain reserved characters.
    static void parseAndValidateWritePoint(const std::string& json);

    // Return the current wall-clock time as nanoseconds since the Unix epoch.
    // Encapsulates the std::chrono boilerplate in one place so callers can
    // capture the value once and reuse it across an entire batch.
    // We use system_clock (not seastar::lowres_clock) because stored timestamps
    // need nanosecond-level precision; lowres_clock only updates every ~10 ms.
    static uint64_t currentNanosTimestamp() {
        auto now = std::chrono::system_clock::now();
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count());
    }
};
