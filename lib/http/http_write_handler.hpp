#ifndef HTTP_WRITE_HANDLER_H_INCLUDED
#define HTTP_WRITE_HANDLER_H_INCLUDED

#include <string>
#include <memory>
#include <variant>
#include <chrono>
#include <glaze/glaze.hpp>

#include "engine.hpp"
#include "tsdb_value.hpp"
#include "series_id.hpp"
#include "wal_file_manager.hpp"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/http/function_handlers.hh>
#include <seastar/http/httpd.hh>
#include <seastar/http/json_path.hh>

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
    static constexpr size_t MAX_WRITE_BODY_SIZE = 64 * 1024 * 1024; // 64MB

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
        std::vector<uint64_t> timestamps;  // Array of timestamps
    };
    
    // Structure for coalescing multiple individual writes into array writes
    struct CoalesceCandidate {
        std::string seriesKey;  // measurement + tags + field for grouping
        std::string measurement;
        std::map<std::string, std::string> tags;
        std::string fieldName;
        TSMValueType valueType;
        std::vector<uint64_t> timestamps;
        
        // Value storage by type (only one will be used based on valueType)
        std::vector<double> doubleValues;
        std::vector<uint8_t> boolValues;
        std::vector<std::string> stringValues;
        
        // Helper to add a value with timestamp
        void addValue(uint64_t timestamp, double value) {
            timestamps.push_back(timestamp);
            doubleValues.push_back(value);
        }
        
        void addValue(uint64_t timestamp, bool value) {
            timestamps.push_back(timestamp);
            boolValues.push_back(value);
        }
        
        void addValue(uint64_t timestamp, const std::string& value) {
            timestamps.push_back(timestamp);
            stringValues.push_back(value);
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
    // Aliases MetadataOp from leveldb_index.hpp for batch indexing compatibility.
    using MetaOp = MetadataOp;

    // Parse a single write point from JSON string
    WritePoint parseWritePoint(const std::string& json);
    
    // Parse a write point that may contain arrays
    MultiWritePoint parseMultiWritePoint(const json_value_t& point);
    
    // Process a single write point - determine type and insert
    seastar::future<> processWritePoint(const WritePoint& point);

    // Process a multi-point write with arrays
    // Accepts seenMF and metaOps for cross-batch deduplication
    seastar::future<AggregatedTimingInfo> processMultiWritePoint(
        const MultiWritePoint& point,
        std::unordered_set<std::string>& seenMF,
        std::vector<MetaOp>& metaOps);

    // Validate that all field arrays have the same length as timestamps
    bool validateArraySizes(const MultiWritePoint& point, std::string& error);
    
    // Coalesce multiple individual writes into efficient array writes
    std::vector<MultiWritePoint> coalesceWrites(const json_value_t::array_t& writes_array);
    
    // Create error response JSON
    std::string createErrorResponse(const std::string& error);
    
    // Create success response JSON
    std::string createSuccessResponse(int pointsWritten);

public:
    HttpWriteHandler(seastar::sharded<Engine>* _engineSharded);

    // Main handler for write requests
    seastar::future<std::unique_ptr<seastar::http::reply>> handleWrite(
        std::unique_ptr<seastar::http::request> req);

    // Register routes with HTTP server
    void registerRoutes(seastar::httpd::routes& r);

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
    static void validateWritePointNames(const std::string& measurement,
                                        const std::map<std::string, std::string>& tags,
                                        const std::map<std::string, FieldValue>& fields);

    // Parse and validate a single write point from JSON string (for testing without Seastar).
    // Throws std::runtime_error if JSON is invalid or names contain reserved characters.
    static void parseAndValidateWritePoint(const std::string& json);
};

#endif // HTTP_WRITE_HANDLER_H_INCLUDED