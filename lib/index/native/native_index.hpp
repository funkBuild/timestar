#ifndef NATIVE_INDEX_H_INCLUDED
#define NATIVE_INDEX_H_INCLUDED

#include "../index_backend.hpp"
#include "../key_encoding.hpp"
#include "compaction.hpp"
#include "index_wal.hpp"
#include "manifest.hpp"
#include "memtable.hpp"
#include "merge_iterator.hpp"
#include "sstable.hpp"
#include "write_batch.hpp"

#include "lru_cache.hpp"
#include "timestar_config.hpp"
#include "timestar_value.hpp"

#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <unordered_map>
#include <unordered_set>

namespace timestar {

// CacheSizeEstimator specializations needed for LRU caches in NativeIndex.
// SeriesId128 and SeriesMetadata estimators are already defined in leveldb_index.hpp.
// We need the SeriesWithMetadata vector estimator for the discovery cache.

}  // namespace timestar

namespace timestar::index {

// Seastar-native LSM-tree based index backend.
// Replaces LevelDB with DMA I/O, no thread-pool crossings.
// Uses the same key schema and wire format as LevelDBIndex for compatibility.
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
    seastar::future<> compact() override;

    // Non-virtual: insert indexing (template, like LevelDBIndex::indexInsert)
    template <class T>
    seastar::future<SeriesId128> indexInsert(const TimeStarInsert<T>& insert);

    // Backward compatibility aliases for code that used LevelDBIndex::FieldStats
    using FieldStats = IndexFieldStats;

    // Static string set encoding helpers (backward compatibility)
    static std::string encodeStringSet(const std::set<std::string>& strings) { return keys::encodeStringSet(strings); }
    static std::set<std::string> decodeStringSet(const std::string& encoded) { return keys::decodeStringSet(encoded); }

private:
    int shardId_;
    std::string indexPath_;

    // --- LSM storage ---
    std::unique_ptr<MemTable> memtable_;
    std::unique_ptr<MemTable> immutableMemtable_;  // Being flushed to SSTable in background
    std::optional<seastar::future<>> flushFuture_;  // Tracks background flush
    std::unique_ptr<IndexWAL> wal_;
    std::unique_ptr<Manifest> manifest_;
    std::unique_ptr<CompactionEngine> compaction_;
    std::vector<std::unique_ptr<SSTableReader>> sstableReaders_;

    // --- Low-level KV operations ---
    // kvGet is synchronous — all data is in memory (MemTable + cached SSTables).
    std::optional<std::string> kvGet(std::string_view key);
    seastar::future<> kvPut(const std::string& key, const std::string& value);
    seastar::future<> kvDelete(const std::string& key);
    seastar::future<> kvWriteBatch(const IndexWriteBatch& batch);

    // Prefix scan: iterate all keys with the given prefix, calling fn for each.
    // fn receives (key, value) and returns true to continue, false to stop.
    // Synchronous — all data is in memory (MemTable + cached SSTables).
    using ScanCallback = std::function<bool(std::string_view key, std::string_view value)>;
    void kvPrefixScan(const std::string& prefix, ScanCallback fn);

    // Non-blocking memtable flush (double-buffered, like LevelDB).
    // maybeFlushMemTable swaps the active memtable to immutable and returns immediately.
    // The actual SSTable write happens asynchronously. If a second flush triggers
    // while the first is still in progress, we wait for it (like LevelDB's imm_ check).
    seastar::future<> maybeFlushMemTable();
    seastar::future<> flushMemTable();
    seastar::future<> doFlushImmutableMemTable();  // Background flush work
    seastar::future<> waitForFlush();  // Wait for any in-flight flush to complete

    // Reopen SSTable readers after flush or compaction.
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
    std::unordered_map<std::string, std::set<std::string>> tagValuesCache_;
    std::unordered_set<std::string> knownFieldTypes_;
    std::unordered_map<std::string, std::vector<SeriesId128>> measurementSeriesCache_;

    timestar::LRUCache<SeriesId128, SeriesMetadata, SeriesId128::Hash> seriesMetadataCache_;
    timestar::LRUCache<std::string, std::shared_ptr<const std::vector<SeriesWithMetadata>>> discoveryCache_;
};

}  // namespace timestar::index

#endif  // NATIVE_INDEX_H_INCLUDED
