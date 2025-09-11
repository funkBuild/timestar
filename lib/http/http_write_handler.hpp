#ifndef __HTTP_WRITE_HANDLER_H_INCLUDED__
#define __HTTP_WRITE_HANDLER_H_INCLUDED__

#include <string>
#include <memory>
#include <variant>
#include <glaze/glaze.hpp>

#include "engine.hpp"
#include "tsdb_value.hpp"
#include "series_id.hpp"

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

class HttpWriteHandler {
private:
    seastar::sharded<Engine>* engineSharded;
    
    // Field value variant for flexible JSON parsing
    using FieldValue = std::variant<double, bool, std::string, int64_t>;
    
    struct WritePoint {
        std::string measurement;
        std::map<std::string, std::string> tags;
        std::map<std::string, FieldValue> fields;
        uint64_t timestamp;
    };
    
    struct FieldArrays {
        std::vector<double> doubles;
        std::vector<bool> bools;
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
        std::vector<bool> boolValues;
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
    
    // Parse a single write point from JSON string
    WritePoint parseWritePoint(const std::string& json);
    
    // Parse a write point that may contain arrays
    MultiWritePoint parseMultiWritePoint(const glz::json_t& point);
    
    // Process a single write point - determine type and insert
    seastar::future<> processWritePoint(const WritePoint& point);
    
    // Process a multi-point write with arrays
    seastar::future<> processMultiWritePoint(const MultiWritePoint& point);
    
    // Validate that all field arrays have the same length as timestamps
    bool validateArraySizes(const MultiWritePoint& point, std::string& error);
    
    // Coalesce multiple individual writes into efficient array writes
    std::vector<MultiWritePoint> coalesceWrites(const glz::json_t::array_t& writes_array);
    
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
};

#endif // __HTTP_WRITE_HANDLER_H_INCLUDED__