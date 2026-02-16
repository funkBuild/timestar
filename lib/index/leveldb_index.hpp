#ifndef LEVELDB_INDEX_H_INCLUDED
#define LEVELDB_INDEX_H_INCLUDED

#include <string>
#include <vector>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <optional>
#include "tsdb_value.hpp"
#include "line_parser.hpp"
#include "series_id.hpp"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <leveldb/db.h>
#include <leveldb/options.h>
#include <leveldb/write_batch.h>
#include <leveldb/filter_policy.h>

// Forward declaration to avoid circular include with tsm.hpp
enum class TSMValueType;

// Metadata operation for batch indexing
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
    FIELD_TYPE = 0x09           // measurement+field -> field type (float, bool, string, integer)
};

// Metadata for a time series
struct SeriesMetadata {
    std::string measurement;
    std::map<std::string, std::string> tags;
    std::string field;
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
    // When the cache exceeds maxSeriesCacheSize, it is cleared entirely.
    // Eviction only causes redundant LevelDB Gets (no data loss).
    static constexpr size_t DEFAULT_MAX_SERIES_CACHE_SIZE = 1'000'000;
    size_t maxSeriesCacheSize = DEFAULT_MAX_SERIES_CACHE_SIZE;
    std::unordered_set<std::string> indexedSeriesCache;

    // In-memory caches for fields and tags to avoid read-modify-write races.
    // In Seastar's single-threaded-per-shard model, synchronous cache access
    // between co_await points eliminates interleaving.
    // These caches are naturally bounded by the number of unique measurements x fields/tags,
    // which is typically small (hundreds to thousands). No eviction needed.
    std::unordered_map<std::string, std::unordered_set<std::string>> fieldsCache;   // measurement -> fields
    std::unordered_map<std::string, std::unordered_set<std::string>> tagsCache;     // measurement -> tag keys
    std::unordered_map<std::string, std::unordered_set<std::string>> tagValuesCache; // measurement+\0+tagKey -> tag values

    // In-memory cache for field types to avoid redundant LevelDB puts on every insert.
    // Key: measurement + '\0' + field, Value: type string ("float", "boolean", "string")
    // Naturally bounded by the number of unique measurement+field combinations. No eviction needed.
    std::unordered_set<std::string> knownFieldTypes;

    // Helper methods for key encoding
    std::string encodeSeriesKey(const std::string& measurement,
                               const std::map<std::string, std::string>& tags,
                               const std::string& field);
    std::string encodeMeasurementFieldsKey(const std::string& measurement);
    std::string encodeMeasurementTagsKey(const std::string& measurement);
    std::string encodeTagValuesKey(const std::string& measurement, const std::string& tagKey);
    std::string encodeSeriesMetadataKey(const SeriesId128& seriesId);
    std::string encodeFieldTypeKey(const std::string& measurement, const std::string& field);

    // Helper methods for value encoding/decoding
    std::string encodeSeriesId(const SeriesId128& seriesId);
    SeriesId128 decodeSeriesId(const std::string& encoded);
    std::string encodeSeriesMetadata(const SeriesMetadata& metadata);
    SeriesMetadata decodeSeriesMetadata(const std::string& encoded);

    // No longer need counter - SeriesId128 generated deterministically from SeriesKey

public:
    static std::string encodeStringSet(const std::set<std::string>& strings);
    static std::set<std::string> decodeStringSet(const std::string& encoded);
    LevelDBIndex(int shardId);
    ~LevelDBIndex();

    seastar::future<> open();
    seastar::future<> close();

    // Series indexing - core functionality
    seastar::future<SeriesId128> getOrCreateSeriesId(std::string measurement,
                                                     std::map<std::string, std::string> tags,
                                                     std::string field);

    seastar::future<std::optional<SeriesId128>> getSeriesId(const std::string& measurement,
                                                            const std::map<std::string, std::string>& tags,
                                                            const std::string& field);

    // Get metadata for a series by ID
    seastar::future<std::optional<SeriesMetadata>> getSeriesMetadata(const SeriesId128& seriesId);

    // Measurement metadata indexing
    seastar::future<> addField(const std::string& measurement, const std::string& field);
    seastar::future<> addTag(const std::string& measurement, const std::string& tagKey, const std::string& tagValue);

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
    seastar::future<SeriesId128> indexInsert(const TSDBInsert<T>& insert);

    // Batch metadata indexing: indexes multiple series in a single LevelDB WriteBatch.
    // Much more efficient than calling indexInsert() in a loop for cold starts / burst writes.
    seastar::future<> indexMetadataBatch(const std::vector<MetadataOp>& ops);

    // Series discovery for queries
    seastar::future<std::vector<SeriesId128>> findSeries(const std::string& measurement,
                                                         const std::map<std::string, std::string>& tagFilters = {});

    // Find series by single tag (optimized)
    seastar::future<std::vector<SeriesId128>> findSeriesByTag(const std::string& measurement,
                                                              const std::string& tagKey,
                                                              const std::string& tagValue);

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

    // Compaction support - get all series for a measurement
    seastar::future<std::vector<SeriesId128>> getAllSeriesForMeasurement(const std::string& measurement);

    // Series cache size management
    void setMaxSeriesCacheSize(size_t maxSize) { maxSeriesCacheSize = maxSize; }
    size_t getMaxSeriesCacheSize() const { return maxSeriesCacheSize; }
    size_t getSeriesCacheSize() const { return indexedSeriesCache.size(); }

    // Debug/maintenance
    seastar::future<size_t> getSeriesCount();
    seastar::future<> compact();
};

#endif
