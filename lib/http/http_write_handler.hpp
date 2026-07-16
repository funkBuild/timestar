#pragma once

#include "content_negotiation.hpp"
#include "engine.hpp"
#include "field_values.hpp"
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
#include <unordered_set>
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

namespace timestar::http {

// Use glz::generic_u64 for JSON parsing to preserve uint64_t timestamp precision.
// glz::json_t (glz::generic) uses num_mode::f64, which parses all numbers as double,
// losing precision for nanosecond timestamps (double has only 53 bits of mantissa,
// while ns timestamps require ~61 bits). generic_u64 stores integers as uint64_t/int64_t.
using json_value_t = glz::generic_u64;

class HttpWriteHandler {
public:
    // Security limit to prevent DoS attacks via large request bodies
    static size_t maxWriteBodySize() { return timestar::config().http.max_write_body_size; }

    // Canonical FieldArrays lives in lib/core/field_values.hpp (shared with
    // query handler and proto converters). Re-export under the private-name
    // alias so existing FieldArrays references in this header / .cpp resolve.
    using FieldArrays = timestar::FieldArrays;

private:
    seastar::sharded<Engine>* engineSharded;

    struct MultiWritePoint {
        std::string measurement;
        // Shared (refcounted, immutable) tag map. std::shared_ptr (atomic
        // refcount) because the same allocation flows into TimeStarInsert's
        // shared tags and crosses Seastar shard boundaries. Producers
        // (parseMultiWritePoint, buildMWPFromFastPath, coalesceWrites) always
        // set it, so consumers can share the pointer instead of deep-copying
        // the map per point.
        std::shared_ptr<const std::map<std::string, std::string>> tags;
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
        // Uses std::shared_ptr (atomic refcount) so the SAME allocation can be
        // handed to MultiWritePoint::tags and onward to TimeStarInsert's shared
        // tags (which cross Seastar shard boundaries) without a deep copy.
        std::shared_ptr<const std::map<std::string, std::string>> sharedTags;
        std::string fieldName;
        TSMValueType valueType;
        std::vector<uint64_t> timestamps;
        uint64_t timestampHashSum = 0;  // commutative sum of all timestamps
        uint64_t timestampHashXor = 0;  // commutative XOR of all timestamps
        uint64_t timestampHashMul = 1;  // multiplicative hash of all timestamps

        // Value storage by type (only one will be used based on valueType)
        std::vector<double> doubleValues;
        std::vector<uint8_t> boolValues;
        std::vector<std::string> stringValues;
        std::vector<int64_t> integerValues;
    };

    // Move a coalesce candidate's typed value vector into a FieldArrays, setting
    // the matching type tag. Shared by the three coalesce-emit sites in
    // coalesceWrites(). The candidate's value vectors are dead afterward.
    static FieldArrays candidateToFieldArrays(CoalesceCandidate& candidate);

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

    // Insert each non-empty typed batch into `engine`, aggregating WAL timing.
    // Shared body of the per-shard insert dispatch (JSON + protobuf paths).
    static seastar::future<AggregatedTimingInfo> insertAllTypes(Engine& engine,
                                                                std::vector<TimeStarInsert<double>> doubles,
                                                                std::vector<TimeStarInsert<bool>> bools,
                                                                std::vector<TimeStarInsert<std::string>> strings,
                                                                std::vector<TimeStarInsert<int64_t>> integers);

    // Dispatch a shard's grouped typed inserts: a direct local call when `shard`
    // is the current shard (avoids the cross-shard message queue — the hot-path
    // fast path), otherwise invoke_on. Used by both write paths.
    static seastar::future<AggregatedTimingInfo> dispatchShardInserts(seastar::sharded<Engine>* engineSharded,
                                                                      unsigned shard,
                                                                      std::vector<TimeStarInsert<double>> doubles,
                                                                      std::vector<TimeStarInsert<bool>> bools,
                                                                      std::vector<TimeStarInsert<std::string>> strings,
                                                                      std::vector<TimeStarInsert<int64_t>> integers);

    // Parse a single write point from JSON string.
    // Parse a write point that may contain arrays.
    // defaultTimestampNs is used when the point has no explicit timestamp,
    // so the caller can capture now() once for an entire batch.
    MultiWritePoint parseMultiWritePoint(const json_value_t& point, uint64_t defaultTimestampNs);

    // Result of processMultiWritePoint: timing info plus any metadata ops
    // that were collected locally (no shared mutable state across coroutines).
    struct WriteResult {
        AggregatedTimingInfo timing;
        std::vector<MetaOp> metaOps;
    };

    // Request-level accumulator for grouped shard dispatch. Holds ONE set of
    // per-shard typed insert vectors shared by all MultiWritePoints in a
    // request, plus the metadata dedup set and per-shard point counts for
    // failure attribution. Consolidating a whole batch here means at most one
    // cross-shard dispatch per shard per request instead of one per
    // MultiWritePoint.
    struct BatchAccumulator {
        std::vector<std::vector<TimeStarInsert<double>>> shardDoubles;
        std::vector<std::vector<TimeStarInsert<bool>>> shardBools;
        std::vector<std::vector<TimeStarInsert<std::string>>> shardStrings;
        std::vector<std::vector<TimeStarInsert<int64_t>>> shardIntegers;
        std::vector<int64_t> shardPoints;  // points routed to each shard (for failure attribution)
        std::unordered_set<SeriesId128, SeriesId128::Hash> seenMF;
        std::vector<MetaOp> metaOps;

        explicit BatchAccumulator(size_t shardCount)
            : shardDoubles(shardCount),
              shardBools(shardCount),
              shardStrings(shardCount),
              shardIntegers(shardCount),
              shardPoints(shardCount, 0) {}

        bool shardEmpty(size_t shard) const {
            return shardDoubles[shard].empty() && shardBools[shard].empty() && shardStrings[shard].empty() &&
                   shardIntegers[shard].empty();
        }
    };

    // Accumulate one MultiWritePoint's per-field inserts into the shared
    // per-shard vectors of `acc` (no dispatch, no suspension). Metadata ops
    // for previously-unseen series are appended to acc.metaOps; acc.seenMF
    // deduplicates across all MultiWritePoints of the request. The point's
    // tags/timestamps/values are moved out.
    static void accumulateMultiWritePoint(MultiWritePoint& point, BatchAccumulator& acc);

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
    // Orchestrates the two phases below.
    std::vector<MultiWritePoint> coalesceWrites(const json_value_t::array_t& writes_array, uint64_t defaultTimestampNs,
                                                size_t& entriesSkippedOut);

    // coalesceWrites phase 1 (candidate grouping): parse writes_array directly
    // from the JSON DOM into per-series CoalesceCandidates. Parse failures
    // increment entriesSkipped; every write examined increments
    // totalWritesProcessed.
    static tsl::robin_map<std::string, CoalesceCandidate> buildCoalesceCandidates(
        const json_value_t::array_t& writes_array, uint64_t defaultTimestampNs, size_t& entriesSkipped,
        size_t& totalWritesProcessed);

    // coalesceWrites phase 2 (field-array assembly): convert candidates into
    // MultiWritePoints, merging same-group candidates with compatible
    // timestamps into multi-field points. Emits sub-MIN_COALESCE_COUNT
    // candidates individually, counted in individualCount. Moves value
    // vectors out of the candidates.
    static std::vector<MultiWritePoint> assembleCoalescedPoints(
        tsl::robin_map<std::string, CoalesceCandidate>& candidates, size_t& individualCount);

    // ── handleWrite phase methods ──
    // All reference parameters (req, bodyStorage, body, pointsWritten,
    // writes_array, rep) are locals of the awaiting handleWrite frame, which
    // stays suspended until these coroutines complete, so the references
    // cannot dangle across suspension points.

    // Phase 1: read the request body (buffered or streamed) and enforce size
    // limits. Returns true with `body` viewing the bytes; returns false with
    // the error response fully assembled in `rep`.
    seastar::future<bool> readWriteBody(seastar::http::request& req, std::string& bodyStorage, std::string_view& body,
                                        timestar::http::WireFormat resFmt, seastar::http::reply& rep);

    // Phase 2: protobuf fast write path; fully assembles the response in `rep`.
    seastar::future<> handleProtobufWrite(std::string_view body, uint64_t defaultTimestampNs,
                                          timestar::http::WireFormat resFmt, seastar::http::reply& rep);

    // Phase 3: fast path for a single write with all-double fields. Returns
    // true when handled (pointsWritten set), false to use the DOM path.
    seastar::future<bool> tryFastDoubleWrite(std::string_view body, uint64_t defaultTimestampNs,
                                             int64_t& pointsWritten);

    // Phase 4: JSON batch write path ("writes" array). Returns true when the
    // response is already assembled in `rep`, false when the caller should
    // emit the shared success response.
    seastar::future<bool> processBatchWrites(const json_value_t::array_t& writes_array, uint64_t defaultTimestampNs,
                                             int64_t& pointsWritten, timestar::http::WireFormat resFmt,
                                             seastar::http::reply& rep);

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

}  // namespace timestar::http

// Backward-compatibility aliases: HttpWriteHandler historically lived in the
// global namespace. New code should use timestar::http:: directly.
using timestar::http::HttpWriteHandler;  // NOLINT(misc-unused-using-decls)
namespace timestar {
using http::HttpWriteHandler;
}  // namespace timestar
