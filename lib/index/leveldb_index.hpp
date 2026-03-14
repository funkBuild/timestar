#ifndef LEVELDB_INDEX_H_INCLUDED
#define LEVELDB_INDEX_H_INCLUDED

#include "index_backend.hpp"
#include "line_parser.hpp"
#include "lru_cache.hpp"
#include "timestar_config.hpp"
#include "timestar_value.hpp"

#include <leveldb/db.h>
#include <leveldb/filter_policy.h>
#include <leveldb/options.h>
#include <leveldb/write_batch.h>

#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <unordered_map>
#include <unordered_set>

// CacheSizeEstimator specializations for LRU cache value types

namespace timestar {

template <>
struct CacheSizeEstimator<SeriesId128> {
    static size_t estimate(const SeriesId128&) { return sizeof(SeriesId128); }
};

template <>
struct CacheSizeEstimator<SeriesMetadata> {
    static size_t estimate(const SeriesMetadata& m) {
        size_t sz = sizeof(SeriesMetadata);
        sz += m.measurement.capacity();
        sz += m.field.capacity();
        // std::map overhead: ~48 bytes per node (key+value+pointers+color)
        for (const auto& [k, v] : m.tags) {
            sz += 48 + k.capacity() + v.capacity();
        }
        return sz;
    }
};

template <>
struct CacheSizeEstimator<std::shared_ptr<const std::vector<SeriesWithMetadata>>> {
    static size_t estimate(const std::shared_ptr<const std::vector<SeriesWithMetadata>>& ptr) {
        if (!ptr)
            return sizeof(std::shared_ptr<const std::vector<SeriesWithMetadata>>);
        // shared_ptr control block (~32 bytes) + vector overhead + per-element size
        size_t sz = 32 + sizeof(std::vector<SeriesWithMetadata>);
        sz += ptr->capacity() * sizeof(SeriesWithMetadata);
        for (const auto& swm : *ptr) {
            // Add heap allocations from strings and maps within each element
            sz += swm.metadata.measurement.capacity();
            sz += swm.metadata.field.capacity();
            for (const auto& [k, v] : swm.metadata.tags) {
                sz += 48 + k.capacity() + v.capacity();
            }
        }
        return sz;
    }
};

}  // namespace timestar

class LevelDBIndex : public IndexBackend {
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
    std::unordered_map<std::string, std::set<std::string>> fieldsCache;     // measurement -> fields
    std::unordered_map<std::string, std::set<std::string>> tagsCache;       // measurement -> tag keys
    std::unordered_map<std::string, std::set<std::string>> tagValuesCache;  // measurement+\0+tagKey -> tag values

    // In-memory cache for field types to avoid redundant LevelDB puts on every insert.
    // Key: measurement + '\0' + field, Value: type string ("float", "boolean", "string")
    // Naturally bounded by the number of unique measurement+field combinations. No eviction needed.
    std::unordered_set<std::string> knownFieldTypes;

    // In-memory cache for MEASUREMENT_SERIES index: measurement -> set of SeriesId128.
    // Populated on first getAllSeriesForMeasurement() call per measurement, then maintained
    // incrementally during inserts. Bounded by the number of unique measurements. Cleared if needed.
    std::unordered_map<std::string, std::vector<SeriesId128>> measurementSeriesCache;

    // Size-aware LRU cache for series metadata (SeriesId128 -> SeriesMetadata).
    // Used to avoid redundant LevelDB Gets during query series discovery.
    // Shard 0 only; other shards use 0-byte capacity (effectively disabled).
    // SeriesMetadata is immutable, so no invalidation is needed.
    timestar::LRUCache<SeriesId128, SeriesMetadata, SeriesId128::Hash> seriesMetadataCache_;

    // Size-aware LRU cache for discovery results (canonical query key -> results).
    // Invalidated per-measurement when new series are created.
    // Shard 0 only; other shards use 0-byte capacity (effectively disabled).
    timestar::LRUCache<std::string, std::shared_ptr<const std::vector<timestar::SeriesWithMetadata>>> discoveryCache_;

    // Helper methods for key encoding
    std::string encodeSeriesKey(const std::string& measurement, const std::map<std::string, std::string>& tags,
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
    ~LevelDBIndex() override;

    seastar::future<> open() override;
    seastar::future<> close() override;

    // Series indexing - core functionality.
    seastar::future<SeriesId128> getOrCreateSeriesId(std::string measurement, std::map<std::string, std::string> tags,
                                                     std::string field) override;

    seastar::future<std::optional<SeriesId128>> getSeriesId(const std::string& measurement,
                                                            const std::map<std::string, std::string>& tags,
                                                            const std::string& field) override;

    // Get metadata for a series by ID
    seastar::future<std::optional<SeriesMetadata>> getSeriesMetadata(const SeriesId128& seriesId) override;

    // Batch metadata lookup: performs all LevelDB Gets in a single seastar::async block,
    // reducing N coroutine suspensions + N thread pool dispatches to just 1 of each.
    seastar::future<std::vector<std::pair<SeriesId128, std::optional<SeriesMetadata>>>> getSeriesMetadataBatch(
        const std::vector<SeriesId128>& seriesIds) override;

    // Measurement metadata indexing
    seastar::future<> addField(const std::string& measurement, const std::string& field) override;
    seastar::future<> addTag(const std::string& measurement, const std::string& tagKey,
                              const std::string& tagValue) override;

    // Batched metadata indexing: checks all field/tag caches synchronously,
    // loads any cache misses in a single LevelDB read, then writes all updates
    // in a single WriteBatch. Reduces N+1 co_awaits to at most 2.
    seastar::future<> addFieldsAndTags(const std::string& measurement, const std::string& field,
                                       const std::map<std::string, std::string>& tags) override;

    // Field type management
    seastar::future<> setFieldType(const std::string& measurement, const std::string& field,
                                    const std::string& type) override;
    seastar::future<std::string> getFieldType(const std::string& measurement, const std::string& field) override;

    // Query support - get metadata for measurements
    seastar::future<std::set<std::string>> getAllMeasurements() override;
    seastar::future<std::set<std::string>> getFields(const std::string& measurement) override;
    seastar::future<std::set<std::string>> getTags(const std::string& measurement) override;
    seastar::future<std::set<std::string>> getTagValues(const std::string& measurement,
                                                         const std::string& tagKey) override;

    // Bulk operations for insert batching (non-virtual, delegates to virtual methods)
    template <class T>
    seastar::future<SeriesId128> indexInsert(const TimeStarInsert<T>& insert);

    // Batch metadata indexing: indexes multiple series in a single LevelDB WriteBatch.
    // Much more efficient than calling indexInsert() in a loop for cold starts / burst writes.
    seastar::future<> indexMetadataBatch(const std::vector<MetadataOp>& ops) override;

    // Series discovery for queries.
    seastar::future<std::expected<std::vector<SeriesId128>, SeriesLimitExceeded>> findSeries(
        const std::string& measurement, const std::map<std::string, std::string>& tagFilters = {},
        size_t maxSeries = 0) override;

    // Backward compatibility alias
    using SeriesWithMetadata = ::timestar::SeriesWithMetadata;

    seastar::future<std::expected<std::vector<SeriesWithMetadata>, SeriesLimitExceeded>> findSeriesWithMetadata(
        const std::string& measurement, const std::map<std::string, std::string>& tagFilters = {},
        const std::unordered_set<std::string>& fieldFilter = {}, size_t maxSeries = 0) override;

    seastar::future<std::expected<std::shared_ptr<const std::vector<SeriesWithMetadata>>, SeriesLimitExceeded>>
    findSeriesWithMetadataCached(const std::string& measurement,
                                 const std::map<std::string, std::string>& tagFilters = {},
                                 const std::unordered_set<std::string>& fieldFilter = {},
                                 size_t maxSeries = 0) override;

    void invalidateDiscoveryCache(const std::string& measurement) override;

    size_t getMetadataCacheSize() const override { return seriesMetadataCache_.size(); }
    size_t getMetadataCacheBytes() const override { return seriesMetadataCache_.currentBytes(); }
    size_t getDiscoveryCacheSize() const override { return discoveryCache_.size(); }
    size_t getDiscoveryCacheBytes() const override { return discoveryCache_.currentBytes(); }

    seastar::future<std::vector<SeriesId128>> findSeriesByTag(const std::string& measurement, const std::string& tagKey,
                                                              const std::string& tagValue,
                                                              size_t maxSeries = 0) override;

    seastar::future<std::vector<SeriesId128>> findSeriesByTagPattern(const std::string& measurement,
                                                                     const std::string& tagKey,
                                                                     const std::string& scopeValue,
                                                                     size_t maxSeries = 0) override;

    seastar::future<std::map<std::string, std::vector<SeriesId128>>> getSeriesGroupedByTag(
        const std::string& measurement, const std::string& tagKey) override;

    // Backward compatibility alias for existing code using LevelDBIndex::FieldStats
    using FieldStats = IndexFieldStats;

    seastar::future<> updateFieldStats(const SeriesId128& seriesId, const std::string& field,
                                        const IndexFieldStats& stats) override;

    seastar::future<std::optional<IndexFieldStats>> getFieldStats(const SeriesId128& seriesId,
                                                                   const std::string& field) override;

    seastar::future<std::expected<std::vector<SeriesId128>, SeriesLimitExceeded>> getAllSeriesForMeasurement(
        const std::string& measurement, size_t maxSeries = 0) override;

    void setMaxSeriesCacheSize(size_t maxSize) override { maxSeriesCacheSize = maxSize; }
    size_t getMaxSeriesCacheSize() const override { return maxSeriesCacheSize; }
    size_t getSeriesCacheSize() const override { return indexedSeriesCache_.size(); }

    seastar::future<> rebuildMeasurementSeriesIndex() override;

    // Retention policy CRUD (shard 0 only)
    seastar::future<> setRetentionPolicy(const RetentionPolicy& policy) override;
    seastar::future<std::optional<RetentionPolicy>> getRetentionPolicy(const std::string& measurement) override;
    seastar::future<std::vector<RetentionPolicy>> getAllRetentionPolicies() override;
    seastar::future<bool> deleteRetentionPolicy(const std::string& measurement) override;

    // Debug/maintenance
    seastar::future<size_t> getSeriesCount() override;
    seastar::future<> compact() override;
};

#endif
