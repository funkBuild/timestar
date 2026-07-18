#pragma once

#include "retention_policy.hpp"
#include "series_id.hpp"

#include <cstdint>
#include <expected>
#include <map>
#include <memory>
#include <optional>
#include <seastar/core/future.hh>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

// Forward declaration to avoid circular include with tsm.hpp
enum class TSMValueType;

// Metadata operation for batch indexing.
struct MetadataOp {
    TSMValueType valueType;
    std::string measurement;
    std::string fieldName;
    std::map<std::string, std::string> tags;
    // Timestamp range of the insert that created this op. Used to record
    // day bitmaps for the FIRST batch of a new series (whose LocalId does
    // not exist yet when the data-shard batch path tries to record days).
    // 0/0 = unknown (day recording skipped).
    uint64_t minTs = 0;
    uint64_t maxTs = 0;
    // Pre-computed series ID (hash of measurement+tags+field). Populated by
    // the write handler where the ID is already known, so downstream shard
    // routing and getOrCreateSeriesId can skip rebuilding + rehashing the
    // series key. Zero (default) = unknown; consumers must fall back to
    // computing it from the components.
    SeriesId128 seriesId;
};

enum IndexKeyType : uint8_t {
    // 0x01 retired (legacy escaped series index)
    MEASUREMENT_FIELDS = 0x02,  // measurement -> fields set
    MEASUREMENT_TAGS = 0x03,    // measurement -> tag keys set
    TAG_VALUES = 0x04,          // measurement+tag_key -> values set
    SERIES_METADATA = 0x05,     // series_id -> metadata
    TAG_INDEX = 0x06,           // measurement+tag_key+tag_value -> series_ids
    // 0x07 retired (GROUP_BY_INDEX — removed in Phase 3)
    FIELD_STATS = 0x08,         // series_id+field -> stats
    FIELD_TYPE = 0x09,          // measurement+field -> field type (float, bool, string, integer)
    MEASUREMENT_SERIES = 0x0A,  // measurement+\0+series_id -> (empty) for fast measurement->series lookup
    RETENTION_POLICY = 0x0B,    // measurement -> JSON retention policy
    // 0x0C was MEASUREMENT_FIELD_SERIES — removed in cleanup; never read.

    // Phase 2: Roaring bitmap postings
    LOCAL_ID_FORWARD = 0x10,  // localId (4B LE) -> SeriesId128 (16B)
    // 0x11 retired (LOCAL_ID_REVERSE — reverse mapping now held in-memory by LocalIdMap)
    LOCAL_ID_COUNTER = 0x12,  // singleton -> next localId counter (4B LE)
    POSTINGS_BITMAP = 0x13,   // measurement\0tagKey\0tagValue -> serialized roaring bitmap

    // Phase 3: Time-scoped postings
    TIME_SERIES_DAY = 0x0D,  // measurement\0day(4B BE — big-endian for lexicographic day ordering)
                             // -> roaring bitmap of active LocalIds

    // Phase 4: Cardinality estimation
    CARDINALITY_HLL = 0x14,     // measurement\0[tagKey\0tagValue] -> HLL registers (16KB)
    MEASUREMENT_BLOOM = 0x15,   // measurement\0 -> serialized bloom filter of all LocalIds
    POSTINGS_WATERMARK = 0x16,  // singleton -> localIdMap.nextId() at last bitmap flush (crash-repair bound)

    // Per-value tag value markers: replace the TAG_VALUES blob write path.
    // One empty-value key per (measurement, tagKey, tagValue) — appending a new
    // value is O(1) instead of re-encoding the whole value set (O(V²) write
    // amplification). Read path unions these with any legacy TAG_VALUES blob.
    TAG_VALUE_MARKER = 0x17,  // measurement+\0+tagKey+\0+tagValue -> (empty)
};

// Metadata for a time series
struct SeriesMetadata {
    std::string measurement;
    std::map<std::string, std::string> tags;
    std::string field;
};

// Error type returned when a series discovery query exceeds the caller's limit.
struct SeriesLimitExceeded {
    size_t discovered;  // Number of series discovered before bailing out
    size_t limit;       // The limit that was exceeded
};

namespace timestar {

// SeriesWithMetadata is used by IndexBackend and callers for discovery results.
struct SeriesWithMetadata {
    SeriesId128 seriesId;
    SeriesMetadata metadata;
};

}  // namespace timestar

// Field statistics for query optimization (used by IndexBackend interface).
struct IndexFieldStats {
    std::string dataType;
    int64_t minTime;
    int64_t maxTime;
    uint64_t pointCount;
};

// Abstract interface for the metadata index backend.
// Implementation: NativeIndex (lib/index/native/native_index.hpp).
class IndexBackend {
public:
    virtual ~IndexBackend() = default;

    // Lifecycle
    virtual seastar::future<> open() = 0;
    virtual seastar::future<> close() = 0;

    // Series indexing - core functionality.
    virtual seastar::future<SeriesId128> getOrCreateSeriesId(std::string measurement,
                                                             std::map<std::string, std::string> tags,
                                                             std::string field) = 0;

    virtual seastar::future<std::optional<SeriesId128>> getSeriesId(const std::string& measurement,
                                                                    const std::map<std::string, std::string>& tags,
                                                                    const std::string& field) = 0;

    // Get metadata for a series by ID
    virtual seastar::future<std::optional<SeriesMetadata>> getSeriesMetadata(const SeriesId128& seriesId) = 0;

    // Batch metadata lookup
    virtual seastar::future<std::vector<std::pair<SeriesId128, std::optional<SeriesMetadata>>>> getSeriesMetadataBatch(
        const std::vector<SeriesId128>& seriesIds) = 0;

    // Field type management
    virtual seastar::future<> setFieldType(const std::string& measurement, const std::string& field,
                                           const std::string& type) = 0;
    virtual seastar::future<std::string> getFieldType(const std::string& measurement, const std::string& field) = 0;

    // Query support - get metadata for measurements
    virtual seastar::future<std::set<std::string>> getAllMeasurements() = 0;
    virtual seastar::future<std::set<std::string>> getFields(std::string measurement) = 0;
    virtual seastar::future<std::set<std::string>> getTags(std::string measurement) = 0;
    virtual seastar::future<std::set<std::string>> getTagValues(std::string measurement, std::string tagKey) = 0;

    // Batch metadata indexing
    virtual seastar::future<> indexMetadataBatch(const std::vector<MetadataOp>& ops) = 0;

    // Series discovery for queries.
    virtual seastar::future<std::expected<std::vector<SeriesId128>, SeriesLimitExceeded>> findSeries(
        const std::string& measurement, const std::map<std::string, std::string>& tagFilters = {},
        size_t maxSeries = 0) = 0;

    using SeriesWithMetadata = ::timestar::SeriesWithMetadata;

    virtual seastar::future<std::expected<std::vector<SeriesWithMetadata>, SeriesLimitExceeded>> findSeriesWithMetadata(
        const std::string& measurement, const std::map<std::string, std::string>& tagFilters = {},
        const std::unordered_set<std::string>& fieldFilter = {}, size_t maxSeries = 0) = 0;

    virtual seastar::future<std::expected<std::shared_ptr<const std::vector<SeriesWithMetadata>>, SeriesLimitExceeded>>
    findSeriesWithMetadataCached(const std::string& measurement,
                                 const std::map<std::string, std::string>& tagFilters = {},
                                 const std::unordered_set<std::string>& fieldFilter = {}, size_t maxSeries = 0) = 0;

    // Invalidate discovery cache entries for a given measurement.
    virtual void invalidateDiscoveryCache(const std::string& measurement) = 0;

    // Cache statistics
    virtual size_t getMetadataCacheSize() const = 0;
    virtual size_t getMetadataCacheBytes() const = 0;
    virtual size_t getDiscoveryCacheSize() const = 0;
    virtual size_t getDiscoveryCacheBytes() const = 0;

    // Find series by single tag (exact match).
    virtual seastar::future<std::vector<SeriesId128>> findSeriesByTag(const std::string& measurement,
                                                                      const std::string& tagKey,
                                                                      const std::string& tagValue,
                                                                      size_t maxSeries = 0) = 0;

    // Group series by tag value for aggregations
    virtual seastar::future<std::map<std::string, std::vector<SeriesId128>>> getSeriesGroupedByTag(
        const std::string& measurement, const std::string& tagKey) = 0;

    // Field statistics
    virtual seastar::future<> updateFieldStats(const SeriesId128& seriesId, const std::string& field,
                                               const IndexFieldStats& stats) = 0;

    virtual seastar::future<std::optional<IndexFieldStats>> getFieldStats(const SeriesId128& seriesId,
                                                                          const std::string& field) = 0;

    // Compaction support - get all series for a measurement.
    virtual seastar::future<std::expected<std::vector<SeriesId128>, SeriesLimitExceeded>> getAllSeriesForMeasurement(
        const std::string& measurement, size_t maxSeries = 0) = 0;

    // Series cache management.
    virtual size_t getSeriesCacheSize() const = 0;

    // Retention policy CRUD
    virtual seastar::future<> setRetentionPolicy(const RetentionPolicy& policy) = 0;
    virtual seastar::future<std::optional<RetentionPolicy>> getRetentionPolicy(const std::string& measurement) = 0;
    virtual seastar::future<std::vector<RetentionPolicy>> getAllRetentionPolicies() = 0;
    virtual seastar::future<bool> deleteRetentionPolicy(const std::string& measurement) = 0;

    // Debug/maintenance
    virtual seastar::future<> compact() = 0;
};
