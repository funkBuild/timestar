#ifndef LEVELDB_INDEX_H_INCLUDED
#define LEVELDB_INDEX_H_INCLUDED

#include <string>
#include <vector>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <optional>
#include "timestar_value.hpp"
#include "line_parser.hpp"
#include "series_id.hpp"
#include "timestar_config.hpp"

#include <expected>

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include "retention_policy.hpp"

#include <leveldb/db.h>
#include <leveldb/options.h>
#include <leveldb/write_batch.h>
#include <leveldb/filter_policy.h>

// Forward declaration to avoid circular include with tsm.hpp
enum class TSMValueType;

// Metadata operation for batch indexing.
struct MetadataOp {
    TSMValueType valueType;
    std::string measurement;
    std::string fieldName;
    std::map<std::string, std::string> tags;
};

enum IndexKeyType : uint8_t {
    SERIES_INDEX = 0x01,        // series_key -> series_id
    MEASUREMENT_FIELDS = 0x02,   // measurement -> fields set
    MEASUREMENT_TAGS = 0x03,     // measurement -> tag keys set
    TAG_VALUES = 0x04,          // measurement+tag_key -> values set
    SERIES_METADATA = 0x05,     // series_id -> metadata
    TAG_INDEX = 0x06,           // measurement+tag_key+tag_value -> series_ids
    GROUP_BY_INDEX = 0x07,      // measurement+tag_key+tag_value -> series_ids (for group-by)
    FIELD_STATS = 0x08,         // series_id+field -> stats
    FIELD_TYPE = 0x09,          // measurement+field -> field type (float, bool, string, integer)
    MEASUREMENT_SERIES = 0x0A,  // measurement+\0+series_id -> (empty) for fast measurement->series lookup
    RETENTION_POLICY = 0x0B     // measurement -> JSON retention policy
};

// Metadata for a time series
struct SeriesMetadata {
    std::string measurement;
    std::map<std::string, std::string> tags;
    std::string field;
};

// Error type returned when a series discovery query exceeds the caller's limit.
// Using std::expected allows callers to distinguish "limit exceeded" from success
// without exceptions (which would be inappropriate for an expected condition).
struct SeriesLimitExceeded {
    size_t discovered;  // Number of series discovered before bailing out
    size_t limit;       // The limit that was exceeded
};

class LevelDBIndex {
private:
    std::unique_ptr<leveldb::DB> db;
    // RAII-managed bloom filter policy to prevent memory leaks.
    // Must be declared after db so that db is destroyed first during destruction,
    // since LevelDB may reference the filter policy during shutdown.
    std::unique_ptr<const leveldb::FilterPolicy> filterPolicy_;
    int shardId;
    std::string indexPath;

    // In-memory cache for indexed series (to avoid redundant LevelDB Gets).
    // Bounded to prevent unbounded memory growth (~120 bytes per entry).
    //
    // Two-generation design to avoid blocking the hot insert path:
    //   - indexedSeriesCache_: active generation, receives new inserts
    //   - indexedSeriesCacheRetired_: previous generation, being incrementally cleared
    //
    // When the active cache exceeds maxSeriesCacheSize, it is swapped into the
    // retired slot (O(1) pointer swap). The retired cache is then drained in small
    // batches (EVICTION_BATCH_SIZE entries per insert) so that no single insert
    // pays the cost of deallocating up to 1M strings. Lookups check both generations
    // so recently-evicted entries still get cache hits until they are drained.
    //
    // Because the active cache needs maxSeriesCacheSize inserts to refill while the
    // retired cache drains at EVICTION_BATCH_SIZE per insert, the retired cache is
    // always fully drained before the next generation swap (the drain takes
    // maxSeriesCacheSize / EVICTION_BATCH_SIZE inserts, which is << maxSeriesCacheSize).
    static size_t defaultMaxSeriesCacheSize() { return timestar::config().index.series_cache_size; }
    static constexpr size_t EVICTION_BATCH_SIZE = 256;
    size_t maxSeriesCacheSize = defaultMaxSeriesCacheSize();
    std::unordered_set<std::string> indexedSeriesCache_;
    std::unordered_set<std::string> indexedSeriesCacheRetired_;

    // Returns true if the key is in either generation of the cache.
    bool seriesCacheContains(const std::string& key) const;
    // Inserts a key into the active cache and performs incremental eviction.
    void seriesCacheInsert(const std::string& key);
    // Inserts a key (move) into the active cache and performs incremental eviction.
    void seriesCacheInsert(std::string&& key);
    // Performs incremental eviction of the retired cache and, if needed,
    // retires the active cache. Called after every insert into the active cache.
    void seriesCacheEvictIncremental();

    // In-memory caches for fields and tags to avoid read-modify-write races.
    // In Seastar's single-threaded-per-shard model, synchronous cache access
    // between co_await points eliminates interleaving.
    // These caches are naturally bounded by the number of unique measurements x fields/tags,
    // which is typically small (hundreds to thousands). No eviction needed.
    std::unordered_map<std::string, std::set<std::string>> fieldsCache;   // measurement -> fields
    std::unordered_map<std::string, std::set<std::string>> tagsCache;     // measurement -> tag keys
    std::unordered_map<std::string, std::set<std::string>> tagValuesCache; // measurement+\0+tagKey -> tag values

    // In-memory cache for field types to avoid redundant LevelDB puts on every insert.
    // Key: measurement + '\0' + field, Value: type string ("float", "boolean", "string")
    // Naturally bounded by the number of unique measurement+field combinations. No eviction needed.
    std::unordered_set<std::string> knownFieldTypes;

    // In-memory cache for MEASUREMENT_SERIES index: measurement -> set of SeriesId128.
    // Populated on first getAllSeriesForMeasurement() call per measurement, then maintained
    // incrementally during inserts. Bounded by the number of unique measurements. Cleared if needed.
    std::unordered_map<std::string, std::vector<SeriesId128>> measurementSeriesCache;

    // Helper methods for key encoding
    std::string encodeSeriesKey(const std::string& measurement,
                               const std::map<std::string, std::string>& tags,
                               const std::string& field);
    std::string encodeMeasurementFieldsKey(const std::string& measurement);
    std::string encodeMeasurementTagsKey(const std::string& measurement);
    std::string encodeTagValuesKey(const std::string& measurement, const std::string& tagKey);
    std::string encodeSeriesMetadataKey(const SeriesId128& seriesId);
    std::string encodeFieldTypeKey(const std::string& measurement, const std::string& field);
    std::string encodeMeasurementSeriesKey(const std::string& measurement, const SeriesId128& seriesId);
    std::string encodeMeasurementSeriesPrefix(const std::string& measurement);

    // Helper methods for value encoding/decoding
    std::string encodeSeriesId(const SeriesId128& seriesId);
    SeriesId128 decodeSeriesId(const std::string& encoded);
    // Zero-copy overload: decode directly from raw pointer (e.g. leveldb::Slice::data())
    SeriesId128 decodeSeriesId(const char* data, size_t len);
    std::string encodeSeriesMetadata(const SeriesMetadata& metadata);
    SeriesMetadata decodeSeriesMetadata(const std::string& encoded);
    // Zero-copy overload: decode directly from raw pointer (e.g. leveldb::Slice::data())
    SeriesMetadata decodeSeriesMetadata(const char* data, size_t len);

    // No longer need counter - SeriesId128 generated deterministically from SeriesKey

public:
    static std::string encodeStringSet(const std::set<std::string>& strings);
    static std::set<std::string> decodeStringSet(const std::string& encoded);
    LevelDBIndex(int shardId);
    ~LevelDBIndex();

    seastar::future<> open();
    seastar::future<> close();

    // Series indexing - core functionality.
    seastar::future<SeriesId128> getOrCreateSeriesId(std::string measurement,
                                                     std::map<std::string, std::string> tags,
                                                     std::string field);

    seastar::future<std::optional<SeriesId128>> getSeriesId(const std::string& measurement,
                                                            const std::map<std::string, std::string>& tags,
                                                            const std::string& field);

    // Get metadata for a series by ID
    seastar::future<std::optional<SeriesMetadata>> getSeriesMetadata(const SeriesId128& seriesId);

    // Batch metadata lookup: performs all LevelDB Gets in a single seastar::async block,
    // reducing N coroutine suspensions + N thread pool dispatches to just 1 of each.
    seastar::future<std::vector<std::pair<SeriesId128, std::optional<SeriesMetadata>>>>
    getSeriesMetadataBatch(const std::vector<SeriesId128>& seriesIds);

    // Measurement metadata indexing
    seastar::future<> addField(const std::string& measurement, const std::string& field);
    seastar::future<> addTag(const std::string& measurement, const std::string& tagKey, const std::string& tagValue);

    // Batched metadata indexing: checks all field/tag caches synchronously,
    // loads any cache misses in a single LevelDB read, then writes all updates
    // in a single WriteBatch. Reduces N+1 co_awaits to at most 2.
    seastar::future<> addFieldsAndTags(const std::string& measurement,
                                       const std::string& field,
                                       const std::map<std::string, std::string>& tags);

    // Field type management
    seastar::future<> setFieldType(const std::string& measurement, const std::string& field, const std::string& type);
    seastar::future<std::string> getFieldType(const std::string& measurement, const std::string& field);

    // Query support - get metadata for measurements
    seastar::future<std::set<std::string>> getAllMeasurements();
    seastar::future<std::set<std::string>> getFields(const std::string& measurement);
    seastar::future<std::set<std::string>> getTags(const std::string& measurement);
    seastar::future<std::set<std::string>> getTagValues(const std::string& measurement, const std::string& tagKey);

    // Bulk operations for insert batching
    template<class T>
    seastar::future<SeriesId128> indexInsert(const TimeStarInsert<T>& insert);

    // Batch metadata indexing: indexes multiple series in a single LevelDB WriteBatch.
    // Much more efficient than calling indexInsert() in a loop for cold starts / burst writes.
    seastar::future<> indexMetadataBatch(const std::vector<MetadataOp>& ops);

    // Series discovery for queries.
    // maxSeries: if > 0, returns SeriesLimitExceeded error when the final result
    // exceeds the limit. This prevents wasted metadata Gets for queries that would
    // be rejected anyway. 0 means unlimited.
    seastar::future<std::expected<std::vector<SeriesId128>, SeriesLimitExceeded>>
    findSeries(const std::string& measurement,
               const std::map<std::string, std::string>& tagFilters = {},
               size_t maxSeries = 0);

    struct SeriesWithMetadata {
        SeriesId128 seriesId;
        SeriesMetadata metadata;
    };

    // maxSeries: if > 0, returns SeriesLimitExceeded error when the series count
    // exceeds the limit. Checked after findSeries() and before metadata Gets,
    // preventing wasted I/O. 0 means unlimited.
    seastar::future<std::expected<std::vector<SeriesWithMetadata>, SeriesLimitExceeded>>
    findSeriesWithMetadata(
        const std::string& measurement,
        const std::map<std::string, std::string>& tagFilters = {},
        const std::unordered_set<std::string>& fieldFilter = {},
        size_t maxSeries = 0);

    // Find series by single tag (optimized, exact match only).
    // maxSeries: if > 0, stops scanning early when the count exceeds the limit.
    // 0 means unlimited. Note: for multi-tag intersection queries, individual tag
    // scans run without limits since intersection reduces the count. The limit is
    // checked on the final result in findSeries().
    seastar::future<std::vector<SeriesId128>> findSeriesByTag(const std::string& measurement,
                                                              const std::string& tagKey,
                                                              const std::string& tagValue,
                                                              size_t maxSeries = 0);

    // Find series by tag pattern (wildcard or regex).
    // Scans TAG_INDEX entries for the given measurement+tagKey, using the literal
    // prefix of the pattern to narrow the LevelDB seek, then post-filters with
    // SeriesMatcher::matchesTag(). Supports *, ?, ~regex, and /regex/ patterns.
    seastar::future<std::vector<SeriesId128>> findSeriesByTagPattern(
        const std::string& measurement,
        const std::string& tagKey,
        const std::string& scopeValue,
        size_t maxSeries = 0);

    // Group series by tag value for aggregations
    seastar::future<std::map<std::string, std::vector<SeriesId128>>>
        getSeriesGroupedByTag(const std::string& measurement, const std::string& tagKey);

    // Field statistics for query optimization
    struct FieldStats {
        std::string dataType;
        int64_t minTime;
        int64_t maxTime;
        uint64_t pointCount;
    };

    seastar::future<> updateFieldStats(const SeriesId128& seriesId, const std::string& field,
                                       const FieldStats& stats);

    seastar::future<std::optional<FieldStats>> getFieldStats(const SeriesId128& seriesId,
                                                              const std::string& field);

    // Compaction support - get all series for a measurement.
    // maxSeries: if > 0, stops scanning early and returns SeriesLimitExceeded
    // when the count exceeds the limit. 0 means unlimited.
    seastar::future<std::expected<std::vector<SeriesId128>, SeriesLimitExceeded>>
    getAllSeriesForMeasurement(const std::string& measurement, size_t maxSeries = 0);

    // Series cache size management.
    // getSeriesCacheSize() returns the active cache size only (not retired entries
    // that are being incrementally evicted). This reflects the "live" working set.
    void setMaxSeriesCacheSize(size_t maxSize) { maxSeriesCacheSize = maxSize; }
    size_t getMaxSeriesCacheSize() const { return maxSeriesCacheSize; }
    size_t getSeriesCacheSize() const { return indexedSeriesCache_.size(); }

    // Rebuild the MEASUREMENT_SERIES index from existing SERIES_METADATA entries.
    // Useful for backward compatibility when upgrading from databases that lack this index.
    seastar::future<> rebuildMeasurementSeriesIndex();

    // Retention policy CRUD (shard 0 only)
    seastar::future<> setRetentionPolicy(const RetentionPolicy& policy);
    seastar::future<std::optional<RetentionPolicy>> getRetentionPolicy(const std::string& measurement);
    seastar::future<std::vector<RetentionPolicy>> getAllRetentionPolicies();
    seastar::future<bool> deleteRetentionPolicy(const std::string& measurement);

    // Debug/maintenance
    seastar::future<size_t> getSeriesCount();
    seastar::future<> compact();
};

#endif
