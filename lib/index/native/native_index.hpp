#pragma once

#include "../index_backend.hpp"
#include "../key_encoding.hpp"
#include "../schema_update.hpp"
#include "block_cache.hpp"
#include "bloom_filter.hpp"
#include "compaction.hpp"
#include "hyperloglog.hpp"
#include "index_wal.hpp"
#include "local_id_map.hpp"
#include "lru_cache.hpp"
#include "manifest.hpp"
#include "memtable.hpp"
#include "merge_iterator.hpp"
#include "sstable.hpp"
#include "timestar_config.hpp"
#include "timestar_value.hpp"
#include "write_batch.hpp"

#include <map>
#include <memory>
#include <roaring.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/smp.hh>
#include <unordered_map>
#include <unordered_set>

namespace timestar {

// CacheSizeEstimator specializations for LRU cache value types
// (used by NativeIndex's internal LRU caches)

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
        size_t sz = 32 + sizeof(std::vector<SeriesWithMetadata>);
        sz += ptr->capacity() * sizeof(SeriesWithMetadata);
        for (const auto& swm : *ptr) {
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

namespace timestar::index {

// Seastar-native LSM-tree based index backend.
// Uses DMA I/O with no thread-pool crossings.
class NativeIndex : public IndexBackend {
public:
    explicit NativeIndex(int shardId);
    ~NativeIndex() override;

    // Lifecycle
    seastar::future<> open() override;
    seastar::future<> close() override;

    // --- Series indexing ---
    seastar::future<SeriesId128> getOrCreateSeriesId(std::string measurement, std::map<std::string, std::string> tags,
                                                     std::string field) override;

    seastar::future<std::optional<SeriesId128>> getSeriesId(const std::string& measurement,
                                                            const std::map<std::string, std::string>& tags,
                                                            const std::string& field) override;

    seastar::future<std::optional<SeriesMetadata>> getSeriesMetadata(const SeriesId128& seriesId) override;

    seastar::future<std::vector<std::pair<SeriesId128, std::optional<SeriesMetadata>>>> getSeriesMetadataBatch(
        const std::vector<SeriesId128>& seriesIds) override;

    // --- Measurement metadata ---
    seastar::future<> addField(const std::string& measurement, const std::string& field) override;
    seastar::future<> addTag(const std::string& measurement, const std::string& tagKey,
                             const std::string& tagValue) override;
    seastar::future<> addFieldsAndTags(const std::string& measurement, const std::string& field,
                                       const std::map<std::string, std::string>& tags) override;

    seastar::future<> setFieldType(const std::string& measurement, const std::string& field,
                                   const std::string& type) override;
    seastar::future<std::string> getFieldType(const std::string& measurement, const std::string& field) override;

    seastar::future<std::set<std::string>> getAllMeasurements() override;
    seastar::future<std::set<std::string>> getFields(const std::string& measurement) override;
    seastar::future<std::set<std::string>> getTags(const std::string& measurement) override;
    seastar::future<std::set<std::string>> getTagValues(const std::string& measurement,
                                                        const std::string& tagKey) override;

    seastar::future<> indexMetadataBatch(const std::vector<MetadataOp>& ops) override;

    // --- Series discovery ---
    seastar::future<std::expected<std::vector<SeriesId128>, SeriesLimitExceeded>> findSeries(
        const std::string& measurement, const std::map<std::string, std::string>& tagFilters = {},
        size_t maxSeries = 0) override;

    seastar::future<std::expected<std::vector<SeriesWithMetadata>, SeriesLimitExceeded>> findSeriesWithMetadata(
        const std::string& measurement, const std::map<std::string, std::string>& tagFilters = {},
        const std::unordered_set<std::string>& fieldFilter = {}, size_t maxSeries = 0) override;

    seastar::future<std::expected<std::shared_ptr<const std::vector<SeriesWithMetadata>>, SeriesLimitExceeded>>
    findSeriesWithMetadataCached(const std::string& measurement,
                                 const std::map<std::string, std::string>& tagFilters = {},
                                 const std::unordered_set<std::string>& fieldFilter = {},
                                 size_t maxSeries = 0) override;

    void invalidateDiscoveryCache(const std::string& measurement) override;

    // --- Cache stats ---
    size_t getMetadataCacheSize() const override;
    size_t getMetadataCacheBytes() const override;
    size_t getDiscoveryCacheSize() const override;
    size_t getDiscoveryCacheBytes() const override;

    // --- Tag queries ---
    seastar::future<std::vector<SeriesId128>> findSeriesByTag(const std::string& measurement, const std::string& tagKey,
                                                              const std::string& tagValue,
                                                              size_t maxSeries = 0) override;

    seastar::future<std::vector<SeriesId128>> findSeriesByTagPattern(const std::string& measurement,
                                                                     const std::string& tagKey,
                                                                     const std::string& scopeValue,
                                                                     size_t maxSeries = 0) override;

    seastar::future<std::map<std::string, std::vector<SeriesId128>>> getSeriesGroupedByTag(
        const std::string& measurement, const std::string& tagKey) override;

    // --- Field stats ---
    seastar::future<> updateFieldStats(const SeriesId128& seriesId, const std::string& field,
                                       const IndexFieldStats& stats) override;
    seastar::future<std::optional<IndexFieldStats>> getFieldStats(const SeriesId128& seriesId,
                                                                  const std::string& field) override;

    // --- Measurement series ---
    seastar::future<std::expected<std::vector<SeriesId128>, SeriesLimitExceeded>> getAllSeriesForMeasurement(
        const std::string& measurement, size_t maxSeries = 0) override;

    // --- Cache management ---
    void setMaxSeriesCacheSize(size_t maxSize) override;
    size_t getMaxSeriesCacheSize() const override;
    size_t getSeriesCacheSize() const override;

    seastar::future<> rebuildMeasurementSeriesIndex() override;

    // --- Retention policies ---
    seastar::future<> setRetentionPolicy(const RetentionPolicy& policy) override;
    seastar::future<std::optional<RetentionPolicy>> getRetentionPolicy(const std::string& measurement) override;
    seastar::future<std::vector<RetentionPolicy>> getAllRetentionPolicies() override;
    seastar::future<bool> deleteRetentionPolicy(const std::string& measurement) override;

    // --- Debug/maintenance ---
    seastar::future<size_t> getSeriesCount() override;
    // Synchronous series count — safe to call from Prometheus gauge lambdas
    // (no coroutine frame, no suspension point).
    size_t getSeriesCountSync() const;
    seastar::future<> compact() override;

    // Non-virtual: insert indexing (template)
    template <class T>
    seastar::future<SeriesId128> indexInsert(const TimeStarInsert<T>& insert);

    // Phase 4: Cardinality estimation
    double estimateMeasurementCardinality(const std::string& measurement);
    double estimateTagCardinality(const std::string& measurement, const std::string& tagKey,
                                  const std::string& tagValue);

    // Phase 3: Time-scoped discovery — prunes inactive series using per-day bitmaps
    seastar::future<std::expected<std::vector<SeriesWithMetadata>, SeriesLimitExceeded>>
    findSeriesWithMetadataTimeScoped(const std::string& measurement,
                                     const std::map<std::string, std::string>& tagFilters,
                                     const std::unordered_set<std::string>& fieldFilter, uint64_t startTimeNs,
                                     uint64_t endTimeNs, size_t maxSeries = 0);

    // Phase 3: Remove day bitmaps for days before cutoffDay (retention cleanup)
    seastar::future<> removeExpiredDayBitmaps(const std::string& measurement, uint32_t cutoffDay);

    // Schema broadcast: index metadata and return schema changes for broadcast
    seastar::future<SchemaUpdate> indexMetadataBatchWithSchema(const std::vector<MetadataOp>& ops);

    // Apply schema updates from other shards into local caches
    void applySchemaUpdate(const SchemaUpdate& update);

    // Backward compatibility alias for code that used FieldStats
    using FieldStats = IndexFieldStats;

    // Static string set encoding helpers (backward compatibility)
    static std::string encodeStringSet(const std::set<std::string>& strings) { return keys::encodeStringSet(strings); }
    static std::set<std::string> decodeStringSet(const std::string& encoded) { return keys::decodeStringSet(encoded); }

private:
    int shardId_;
    std::string indexPath_;

    // --- LSM storage ---
    std::unique_ptr<MemTable> memtable_;
    std::unique_ptr<MemTable> immutableMemtable_;   // Being flushed to SSTable in background
    std::optional<seastar::future<>> flushFuture_;  // Tracks background flush
    std::unique_ptr<IndexWAL> wal_;
    std::unique_ptr<Manifest> manifest_;
    std::unique_ptr<CompactionEngine> compaction_;
    // Step 4: Map-keyed SSTable readers for incremental refresh
    std::map<uint64_t, std::unique_ptr<SSTableReader>> sstableReaders_;
    // Step 2: Shared block cache for decompressed SSTable data blocks
    BlockCache blockCache_;

    // --- Low-level KV operations ---
    // kvGet is synchronous — all data is in memory (MemTable + cached SSTables).
    std::optional<std::string> kvGet(std::string_view key);
    // Step 8: Existence check without copying the value.
    bool kvExists(std::string_view key);
    seastar::future<> kvPut(const std::string& key, const std::string& value);
    seastar::future<> kvDelete(const std::string& key);
    seastar::future<> kvWriteBatch(const IndexWriteBatch& batch);

    // Prefix scan: iterate all keys with the given prefix, calling fn for each.
    // fn receives (key, value) and returns true to continue, false to stop.
    // Synchronous — all data is in memory (MemTable + cached SSTables).
    using ScanCallback = std::function<bool(std::string_view key, std::string_view value)>;
    void kvPrefixScan(const std::string& prefix, ScanCallback fn);

    // Non-blocking memtable flush (double-buffered).
    // maybeFlushMemTable swaps the active memtable to immutable and returns immediately.
    // The actual SSTable write happens asynchronously. If a second flush triggers
    // while the first is still in progress, we wait for it.
    seastar::future<> maybeFlushMemTable();
    seastar::future<> flushMemTable();
    seastar::future<> doFlushImmutableMemTable();  // Background flush work
    seastar::future<> waitForFlush();              // Wait for any in-flight flush to complete

    // Step 4: Incremental SSTable refresh — only opens new files and closes removed ones.
    seastar::future<> refreshSSTables();
    std::string sstFilename(uint64_t fileNumber);

    // --- Application-level caches ---
    static size_t defaultMaxSeriesCacheSize() { return timestar::config().index.series_cache_size; }
    static constexpr size_t EVICTION_BATCH_SIZE = 256;
    size_t maxSeriesCacheSize_ = defaultMaxSeriesCacheSize();
    std::unordered_set<SeriesId128, SeriesId128::Hash> indexedSeriesCache_;
    std::unordered_set<SeriesId128, SeriesId128::Hash> indexedSeriesCacheRetired_;
    bool seriesCacheContains(const SeriesId128& id) const;
    void seriesCacheInsert(const SeriesId128& id);
    void seriesCacheEvictIncremental();

    std::unordered_map<std::string, std::set<std::string>> fieldsCache_;
    std::unordered_map<std::string, std::set<std::string>> tagsCache_;
    // Bounded tag values cache: cleared when exceeding limit (repopulated on miss from KV).
    std::unordered_map<std::string, std::set<std::string>> tagValuesCache_;
    static constexpr size_t MAX_TAG_VALUES_CACHE_ENTRIES = 256;
    std::unordered_set<std::string> knownFieldTypes_;
    std::unordered_map<std::string, std::string> fieldTypeValues_;  // "meas\0field" → type (from local + broadcast)
    // Bounded schema caches: clear when exceeding limit (repopulated on miss from KV)
    static constexpr size_t MAX_SCHEMA_CACHE_ENTRIES = 2000;
    void trimSchemaCaches();
    SchemaUpdate pendingSchemaUpdate_;  // Accumulates schema changes during indexMetadataBatchWithSchema
    // Step 5: measurementSeriesCache_ REMOVED — getAllSeriesForMeasurement() uses prefix scan directly

    timestar::LRUCache<SeriesId128, SeriesMetadata, SeriesId128::Hash> seriesMetadataCache_;
    timestar::LRUCache<std::string, std::shared_ptr<const std::vector<SeriesWithMetadata>>> discoveryCache_;
    std::unordered_map<std::string, uint64_t> discoveryCacheGen_;  // Per-measurement generation counter
    uint64_t nextDiscoveryCacheGen_ = 1;

    // --- Phase 2: Roaring bitmap postings ---
    LocalIdMap localIdMap_;
    uint32_t lastFlushedLocalId_ = 0;  // LOCAL_ID_FORWARD entries flushed up to (exclusive)

    // Cached bitmap entry: tracks whether modified since last flush.
    struct BitmapEntry {
        roaring::Roaring bitmap;
        bool dirty = false;  // true if modified since last flushDirtyBitmaps()
    };
    // In-memory bitmap cache. Key: "measurement\0tagKey\0tagValue"
    // Populated lazily on first access (insert or query), flushed before memtable swap.
    tsl::robin_map<std::string, BitmapEntry> bitmapCache_;

    // Get or load a bitmap (read-only). Returns nullptr if not found anywhere.
    // Uses pre-built cache key to avoid double string construction.
    const roaring::Roaring* getPostingsBitmapByKey(const std::string& cacheKey);
    // Get or load a bitmap for insert (mutable). Marks entry dirty.
    // cacheKey is consumed on cache miss (moved into map).
    roaring::Roaring& getOrLoadBitmapForInsert(std::string& cacheKey);
    // Flush dirty bitmaps + batched LOCAL_ID_FORWARD entries into the KV store.
    void flushDirtyBitmaps(IndexWriteBatch& batch);
    // Migration: build LocalIdMap + bitmaps from existing TAG_INDEX data on first open.
    void migrateToLocalIds(IndexWriteBatch& batch);
    // Build a bitmap cache key: "measurement\0tagKey\0tagValue"
    static void buildBitmapCacheKey(std::string& out, const std::string& measurement, const std::string& tagKey,
                                    const std::string& tagValue);

    // --- Phase 4: Cardinality estimation ---
    // HLL caches. Key: "measurement\0" (per-measurement) or "measurement\0tagKey\0tagValue" (per-tag-value)
    tsl::robin_map<std::string, HyperLogLog> hllCache_;
    std::unordered_set<std::string> hllCacheDirty_;  // Keys modified since last flush
    // Per-measurement bloom filter of all LocalIds (for short-circuiting non-existent tag lookups)
    tsl::robin_map<std::string, BloomFilter> measurementBloomCache_;
    std::unordered_set<std::string> dirtyMeasurementBlooms_;
    std::unordered_set<std::string> bloomFullyBuilt_;        // Measurements where bloom KV scan already done
    static constexpr size_t MAX_BLOOM_CACHE_ENTRIES = 5000;  // ~40MB at 8KB per bloom
    void trimMeasurementBloomCache();

    void updateHLL(const std::string& measurement, uint32_t localId);
    void updateTagHLL(const std::string& measurement, const std::string& tagKey, const std::string& tagValue,
                      uint32_t localId);
    void flushDirtyHLLs(IndexWriteBatch& batch);
    void flushDirtyMeasurementBlooms(IndexWriteBatch& batch);
    // Step 7: Trim HLL cache after flush — evict non-dirty entries when too large
    void trimHllCache();
    static constexpr size_t MAX_HLL_CACHE_ENTRIES = 1000;

    // --- Phase 3: Time-scoped per-day bitmaps ---
    tsl::robin_map<std::string, BitmapEntry> dayBitmapCache_;

    static void buildDayBitmapCacheKey(std::string& out, const std::string& measurement, uint32_t day);
    roaring::Roaring& getOrLoadDayBitmapForInsert(std::string& cacheKey);
    const roaring::Roaring* getDayBitmapByKey(const std::string& cacheKey);
    void flushDirtyDayBitmaps(IndexWriteBatch& batch);
    roaring::Roaring buildActiveSeriesBitmap(const std::string& measurement, uint32_t startDay, uint32_t endDay);

    // Step 7: Cache eviction — bounded by both entry count and byte budget.
    // Byte budget prevents high-cardinality bitmaps from consuming excessive memory.
    static constexpr size_t MAX_BITMAP_CACHE_ENTRIES = 100000;
    static constexpr size_t MAX_BITMAP_CACHE_BYTES = 128 * 1024 * 1024;  // 128MB per shard
    static constexpr size_t MAX_DAY_BITMAP_CACHE_ENTRIES = 50000;
    static constexpr size_t MAX_DAY_BITMAP_CACHE_BYTES = 64 * 1024 * 1024;  // 64MB per shard
    void trimBitmapCache();
    void trimDayBitmapCache();
    // Step 6: Evict oldest tag values cache entries when over limit
    void trimTagValuesCache();
};

}  // namespace timestar::index
