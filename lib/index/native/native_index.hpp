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
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/timer.hh>
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

    // Fast overload for callers that already know the series ID (write handler
    // pre-computes it for shard routing). Skips buildSeriesKey + rehash, and
    // takes the components by const reference so the common already-indexed
    // path performs no map/string copies; components are copied only on the
    // new-series branch. `knownId` MUST equal
    // SeriesId128::fromSeriesKey(buildSeriesKey(measurement, tags, field)).
    seastar::future<SeriesId128> getOrCreateSeriesId(SeriesId128 knownId, const std::string& measurement,
                                                     const std::map<std::string, std::string>& tags,
                                                     const std::string& field);

    seastar::future<std::optional<SeriesId128>> getSeriesId(const std::string& measurement,
                                                            const std::map<std::string, std::string>& tags,
                                                            const std::string& field) override;

    seastar::future<std::optional<SeriesMetadata>> getSeriesMetadata(const SeriesId128& seriesId) override;

    seastar::future<std::vector<std::pair<SeriesId128, std::optional<SeriesMetadata>>>> getSeriesMetadataBatch(
        const std::vector<SeriesId128>& seriesIds) override;

    // --- Per-series value-type binding (SERIES_VALUE_TYPE, 0x18) ---
    //
    // Not part of the IndexBackend interface: this is enforced by Engine on the
    // owning shard, which holds a concrete NativeIndex. Shard-local, never
    // broadcast — see index_backend.hpp for why the binding exists.
    seastar::future<std::optional<TSMValueType>> getSeriesValueType(const SeriesId128& seriesId);
    seastar::future<> putSeriesValueType(const SeriesId128& seriesId, TSMValueType type);
    seastar::future<> removeSeriesValueType(const SeriesId128& seriesId);

    // --- Measurement metadata ---
    seastar::future<> setFieldType(const std::string& measurement, const std::string& field,
                                   const std::string& type) override;
    seastar::future<std::string> getFieldType(const std::string& measurement, const std::string& field) override;

    seastar::future<std::set<std::string>> getAllMeasurements() override;
    seastar::future<std::set<std::string>> getFields(std::string measurement) override;
    seastar::future<std::set<std::string>> getTags(std::string measurement) override;
    seastar::future<std::set<std::string>> getTagValues(std::string measurement, std::string tagKey) override;

    seastar::future<> indexMetadataBatch(const std::vector<MetadataOp>& ops) override;

    // Day-bitmap recording for time-scoped discovery (0x0D postings).
    // recordInsertDays: exact days from a batch's timestamps (data-shard path).
    // recordDaySpan: [minTs, maxTs] day-span superset (first batch of a new
    // series, driven by MetadataOp). Both no-op when the LocalId is absent.
    seastar::future<> recordInsertDays(const std::string& measurement, const SeriesId128& seriesId,
                                       const std::vector<uint64_t>& timestamps);
    seastar::future<> recordDaySpan(const std::string& measurement, const SeriesId128& seriesId, uint64_t minTs,
                                    uint64_t maxTs);

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
    size_t getSeriesCacheSize() const override;

    // --- Retention policies ---
    seastar::future<> setRetentionPolicy(const RetentionPolicy& policy) override;
    seastar::future<std::optional<RetentionPolicy>> getRetentionPolicy(const std::string& measurement) override;
    seastar::future<std::vector<RetentionPolicy>> getAllRetentionPolicies() override;
    seastar::future<bool> deleteRetentionPolicy(const std::string& measurement) override;

    // --- Debug/maintenance ---
    // Synchronous series count — safe to call from Prometheus gauge lambdas
    // (no coroutine frame, no suspension point).
    size_t getSeriesCountSync() const;
    seastar::future<> compact() override;

    // Non-virtual: insert indexing (template)
    template <class T>
    seastar::future<SeriesId128> indexInsert(const TimeStarInsert<T>& insert);

    // Phase 4: Cardinality estimation
    seastar::future<double> estimateMeasurementCardinality(const std::string& measurement);
    seastar::future<double> estimateTagCardinality(const std::string& measurement, const std::string& tagKey,
                                                   const std::string& tagValue);

    // Phase 3: Time-scoped discovery — prunes inactive series using per-day bitmaps
    seastar::future<std::expected<std::vector<SeriesWithMetadata>, SeriesLimitExceeded>>
    findSeriesWithMetadataTimeScoped(const std::string& measurement,
                                     const std::map<std::string, std::string>& tagFilters,
                                     const std::unordered_set<std::string>& fieldFilter, uint64_t startTimeNs,
                                     uint64_t endTimeNs, size_t maxSeries = 0);

    // Cached variant of findSeriesWithMetadataTimeScoped — shares discoveryCache_
    // with findSeriesWithMetadataCached (cache key additionally scoped by
    // start/end day). Returns a shared immutable vector so repeated day-scoped
    // discovery pays the metadata deep-copy once per cache fill, not per query.
    seastar::future<std::expected<std::shared_ptr<const std::vector<SeriesWithMetadata>>, SeriesLimitExceeded>>
    findSeriesWithMetadataTimeScopedCached(const std::string& measurement,
                                           const std::map<std::string, std::string>& tagFilters,
                                           const std::unordered_set<std::string>& fieldFilter, uint64_t startTimeNs,
                                           uint64_t endTimeNs, size_t maxSeries = 0);

    // Phase 3: Remove day bitmaps for days before cutoffDay (retention cleanup)
    seastar::future<> removeExpiredDayBitmaps(const std::string& measurement, uint32_t cutoffDay);

    // One series' time bounds as the storage layer knows them. Engine fills
    // these from the TSM sparse index (which carries [minTime, maxTime] per
    // series per file but no measurement name) and hands them here, where the
    // seriesId -> measurement/LocalId join lives.
    struct SeriesTimeBounds {
        SeriesId128 seriesId;
        uint64_t minTs;
        uint64_t maxTs;
    };

    // Reconstruct day-bitmap membership for [fromDay, toDay] from storage-layer
    // time bounds. Day membership is otherwise unrecoverable after an unclean
    // shutdown: it lives in dayBitmapCache_ until a flush, and unlike postings
    // it cannot be derived from series metadata (it is a function of insert
    // TIMESTAMPS, which the index never persists per series).
    //
    // The result is a SUPERSET: TSM gives a span, not per-day membership, so a
    // series with a gap is marked present on days it never wrote to. That costs
    // discovery precision only — the data path then returns no points for those
    // days — whereas a missing day silently drops the series from every query
    // whose range starts inside it. Superset is the safe direction.
    //
    // Returns the number of (series, day) memberships added.
    seastar::future<uint64_t> rebuildDayBitmapsFromBounds(const std::vector<SeriesTimeBounds>& bounds, uint32_t fromDay,
                                                          uint32_t toDay);

    // True when the previous session ended in close() with its final flush
    // intact. The day-bitmap repair is only needed after an unclean exit, so
    // this is what keeps a 32-day rescan off every ordinary restart. It is
    // deliberately fail-safe: anything unexpected (missing key, failed flush,
    // first ever boot) reads as UNCLEAN and repairs.
    bool openedCleanly() const { return openedCleanly_; }

    // Test/ops hook: make close() behave like a process that died — skip the
    // clean-shutdown marker. Modelling a crash otherwise requires killing the
    // process, which a test cannot do.
    void suppressCleanShutdownMarker() { suppressCleanMarker_ = true; }

    // Day through which day-bitmap membership was durable at the last flush;
    // nullopt when nothing has ever been flushed. Bounds the rebuild above.
    std::optional<uint32_t> dayBitmapWatermark() const { return dayBitmapWatermark_; }

    // Persist dirty day bitmaps now (timer callback; also called by close()).
    seastar::future<> flushDayBitmapsNow();

    // Schema broadcast: index metadata and return schema changes for broadcast
    seastar::future<SchemaUpdate> indexMetadataBatchWithSchema(const std::vector<MetadataOp>& ops);

    // Apply schema updates from other shards into local caches AND persist
    // them to this shard's KV store, making every shard a complete schema
    // replica (fields/tags blobs via read-modify-write union; tag values via
    // per-value TAG_VALUE_MARKER keys). Idempotent — re-applying a delta
    // (including the origin shard's own) is a harmless union no-op.
    seastar::future<> applySchemaUpdate(SchemaUpdate update);

    // Rebuild and persist the bloom of every measurement currently marked
    // dirty. open() marks them all, so a fresh process serves correct blooms
    // from its first request; nothing else has to wait for a memtable flush.
    seastar::future<> rebuildMeasurementBlooms();

private:
    // Test seam: lets a unit test plant a deliberately stale persisted bloom
    // to prove open() repairs it. Not used by production code.
    friend struct NativeIndexTestAccess;

    int shardId_;
    std::string indexPath_;

    // --- LSM storage ---
    // shared_ptr: kvPrefixScan sources co-own the memtables so a background
    // flush completing (immutableMemtable_.reset()) or a concurrent swap can't
    // destroy the map under a suspended scan.
    std::shared_ptr<MemTable> memtable_;
    std::shared_ptr<MemTable> immutableMemtable_;  // Being flushed to SSTable in background

    // shared_future: multiple coroutines may wait on the same in-flight flush
    // (a plain future is single-consumer — the second waiter hit a moved-from
    // future). Guarded by flushMutex_ for the swap/rotate/schedule region.
    std::optional<seastar::shared_future<>> flushFuture_;

    // Serializes the check→swap→rotate→schedule region of maybeFlushMemTable/
    // flushMemTable. Two coroutines crossing the threshold concurrently would
    // both swap memtable_ into immutableMemtable_, destroying unflushed data
    // and double-rotating the WAL.
    seastar::semaphore flushMutex_{1};

    // Periodic WAL durability sync: append() only buffers, so without this an
    // acknowledged index write could sit in user-space memory indefinitely.
    // The timer bounds the loss window to ~one interval; the gate drains any
    // in-flight sync before close(). ~100ms trades one 4KB DMA write + fsync
    // per interval (only when dirty) for crash durability.
    static constexpr std::chrono::milliseconds kWalSyncInterval{100};
    seastar::timer<> walSyncTimer_;
    seastar::gate walSyncGate_;
    std::unique_ptr<IndexWAL> wal_;
    std::unique_ptr<Manifest> manifest_;
    std::unique_ptr<CompactionEngine> compaction_;
    // Step 4: Map-keyed SSTable readers for incremental refresh.
    // shared_ptr for lifetime safety across co_await in kvGet/kvExists/kvPrefixScan.
    std::map<uint64_t, std::shared_ptr<SSTableReader>> sstableReaders_;
    // Readers removed from sstableReaders_ that in-flight scans may still hold
    // (snapshotting the shared_ptr protects the object, but an eager close()
    // would pull the fd out from under a suspended scan). They are close()d
    // only once the last external reference is gone — drained on each
    // refreshSSTables() and force-drained in close().
    std::vector<std::shared_ptr<SSTableReader>> pendingCloseReaders_;
    seastar::future<> drainPendingCloseReaders(bool force);
    // Step 2: Shared block cache for decompressed SSTable data blocks
    BlockCache blockCache_;
    // Concurrency limiter for SSTable cache-miss block reads (DMA I/O).
    seastar::semaphore blockReadSemaphore_{16};

    // --- Low-level KV operations ---
    // kvGet checks MemTable (sync) then SSTables (async DMA on cache miss).
    seastar::future<std::optional<std::string>> kvGet(std::string_view key);
    // Step 8: Existence check without copying the value.
    seastar::future<bool> kvExists(std::string_view key);
    seastar::future<> kvPut(const std::string& key, const std::string& value);
    seastar::future<> kvDelete(const std::string& key);
    seastar::future<> kvWriteBatch(const IndexWriteBatch& batch);

    // Prefix scan: iterate all keys with the given prefix, calling fn for each.
    // fn receives (key, value) and returns true to continue, false to stop.
    // Async — SSTable block reads may require DMA I/O on cache miss.
    using ScanCallback = std::function<bool(std::string_view key, std::string_view value)>;
    seastar::future<> kvPrefixScan(const std::string& prefix, ScanCallback fn);

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
    // Divided by shard count, like blockCache_/seriesMetadataCache_/discoveryCache_
    // already are. Left undivided this was 1M entries PER SHARD (~40 B/entry,
    // and two generations are live), so a 16-shard box held 32M entries / 1.28 GB
    // for this one set while its three sibling caches were correctly scaled down.
    //
    // NOTE this re-interprets the config value as per-SERVER, not per-shard.
    // Floored so a small configured value on a many-shard box cannot
    // degenerate the two-generation cache to near-zero entries, where every
    // insert would swap generations and re-run the kvExists metadata probe.
    static size_t defaultMaxSeriesCacheSize() {
        return std::max<size_t>(1024, timestar::config().index.series_cache_size / std::max(1u, seastar::smp::count));
    }
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
    static constexpr size_t MAX_TAG_VALUES_CACHE_ENTRIES = 4096;
    // Full tag-value load: union of the legacy TAG_VALUES blob (old DBs, no
    // migration needed) and a prefix scan over TAG_VALUE_MARKER keys.
    seastar::future<std::set<std::string>> loadTagValuesFromKv(const std::string& measurement,
                                                               const std::string& tagKey);
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
    // Shared cache-key builder for findSeriesWithMetadataCached and the
    // time-scoped variant (which appends a day-range suffix).
    std::string buildDiscoveryCacheKey(const std::string& measurement,
                                       const std::map<std::string, std::string>& tagFilters,
                                       const std::unordered_set<std::string>& fieldFilter, size_t maxSeries);

    // --- Phase 2: Roaring bitmap postings ---
    LocalIdMap localIdMap_;
    uint32_t lastFlushedLocalId_ = 0;  // LOCAL_ID_FORWARD entries flushed up to (exclusive)

    // Cached bitmap entry: tracks whether modified since last flush.
    struct BitmapEntry {
        roaring::Roaring bitmap;
        bool dirty = false;  // true if modified since last flushDirtyBitmaps()
        // True between an insert publishing this entry and its KV load
        // completing. The entry is deliberately visible while empty so
        // concurrent inserts can add to it — but a READER that takes it at face
        // value during that window sees "no such series". Readers must merge
        // the persisted bytes in themselves instead of trusting it.
        bool loading = false;
        // Approximate serialized size, refreshed at load/flush time. Used by
        // the trim byte budgets so they don't have to walk every bitmap's
        // container list (getSizeInBytes) on every flush.
        size_t approxBytes = 0;
    };
    // In-memory bitmap cache. Key: "measurement\0tagKey\0tagValue"
    // Populated lazily on first access (insert or query), flushed before memtable swap.
    tsl::robin_map<std::string, BitmapEntry> bitmapCache_;
    // Keys of dirty entries (mirrors hllCacheDirty_): flushes iterate this
    // set instead of walking the entire cache (up to 100K entries) per flush.
    std::unordered_set<std::string> bitmapCacheDirtyKeys_;

    // Get or load a bitmap (read-only). Returns nullptr if not found anywhere.
    // Uses pre-built cache key to avoid double string construction.
    seastar::future<const roaring::Roaring*> getPostingsBitmapByKey(const std::string& cacheKey);
    // Get or load a bitmap for insert (mutable). Marks entry dirty.
    // cacheKey is consumed on cache miss (moved into map).
    seastar::future<roaring::Roaring*> getOrLoadBitmapForInsert(std::string& cacheKey);
    // Flush dirty bitmaps + batched LOCAL_ID_FORWARD entries into the KV store.
    // The flushDirty* family clears dirty state SYNCHRONOUSLY while filling the
    // batch, but nothing is durable until the caller's wal_->append() returns.
    // Each optionally records what it cleared so a failed append can put it
    // back — without that, a full disk leaves the new membership in RAM marked
    // clean: never rewritten, evictable back to the stale persisted value, and
    // invisible to queries with no crash and no error anyone sees.
    void flushDirtyBitmaps(IndexWriteBatch& batch, std::vector<std::string>* flushedKeys = nullptr);
    // Migration: build LocalIdMap + bitmaps from existing TAG_INDEX data on first open.
    seastar::future<> migrateToLocalIds(IndexWriteBatch& batch);
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
    // Count-bounding this was wrong: BloomFilter::build() sizes from the number
    // of distinct tag values in the measurement, so one entry ranges from 8 KB to
    // megabytes. At 5000 entries the "~40MB" assumed a fixed 8 KB entry; at the
    // code's own stated "~100K keys in practice" it is 937 MB, i.e. the entire
    // arena. Bounded by bytes now, like bitmapCache_ already is.
    static constexpr size_t MAX_BLOOM_CACHE_ENTRIES = 5000;
    void trimMeasurementBloomCache();

    seastar::future<> updateHLL(const std::string& measurement, uint32_t localId);
    // Per-tag-value cardinality sketch. Maintained ONLY once the tag value's
    // exact bitmap reaches kTagHllMinCardinality: each sketch is 16 KB, so one
    // per distinct value is ruinous for a high-cardinality tag, and below the
    // threshold the exact bitmap is both cheaper and more accurate.
    // `seedBitmapKey` names the tag value's postings bitmap in bitmapCache_;
    // when the sketch is not yet in hllCache_, the bitmap's ids are merged in
    // (idempotent) so the sketch never under-counts ids that predate it. A
    // KEY is passed rather than a Roaring* because this coroutine suspends
    // before seeding — a raw pointer into bitmapCache_ (a robin_map) would
    // dangle across the suspension (rehash/trim). The caller must co_await
    // this call before mutating the key buffer.
    seastar::future<> updateTagHLL(const std::string& measurement, const std::string& tagKey,
                                   const std::string& tagValue, uint32_t localId, const std::string& seedBitmapKey);
    void flushDirtyHLLs(IndexWriteBatch& batch, std::vector<std::string>* flushedKeys = nullptr);
    seastar::future<> flushDirtyMeasurementBlooms(IndexWriteBatch& batch,
                                                  std::unordered_set<std::string>* flushedMeasurements = nullptr);

    // Everything one flush cleared, so a failed append can undo it as a unit.
    struct FlushRollback {
        std::vector<std::string> bitmapKeys;
        std::vector<std::string> dayBitmapKeys;
        std::vector<std::string> hllKeys;
        std::unordered_set<std::string> bloomMeasurements;
        uint32_t lastFlushedLocalId = 0;
        std::optional<uint32_t> dayBitmapWatermark;
    };
    void restoreAfterFailedFlush(const FlushRollback& rollback);
    // Step 7: Trim HLL cache after flush — evict non-dirty entries when too large
    void trimHllCache();
    static constexpr size_t MAX_HLL_CACHE_ENTRIES = 1000;
    // Below this many series sharing one (measurement, tagKey, tagValue), the
    // exact roaring bitmap answers cardinality queries directly, so a 16 KB
    // sketch with ~0.8% error would cost memory to be LESS accurate.
    static constexpr uint64_t kTagHllMinCardinality = 10000;

    // --- Phase 3: Time-scoped per-day bitmaps ---
    tsl::robin_map<std::string, BitmapEntry> dayBitmapCache_;
    std::unordered_set<std::string> dayBitmapCacheDirtyKeys_;  // see bitmapCacheDirtyKeys_

    static void buildDayBitmapCacheKey(std::string& out, const std::string& measurement, uint32_t day);

    // Highest day any recorded membership has touched this session, and the
    // day through which membership is known durable (persisted by
    // flushDirtyDayBitmaps, reloaded at open). Day-shaped rather than
    // wall-clock so backfill and tests bound the same way live ingest does.
    uint32_t maxRecordedDay_ = 0;
    std::optional<uint32_t> dayBitmapWatermark_;
    bool openedCleanly_ = false;
    bool suppressCleanMarker_ = false;
    seastar::timer<> dayBitmapFlushTimer_;
    seastar::gate dayBitmapFlushGate_;
    // Advance the "highest day recorded" mark, ignoring days implausibly far in
    // the future. Timestamps are raw client input: one point stamped years ahead
    // (an unset or skewed device clock) would otherwise pin the watermark there
    // for a decade, so every later flush would consider it current and the
    // repair window would sit where no data lives. A week of slack covers real
    // skew without letting a garbage timestamp through.
    void noteRecordedDay(uint32_t day);
    static constexpr uint32_t kMaxFutureWatermarkDays = 7;
    // True when the persisted watermark trails what is now durable. A flush is
    // worth doing for this alone: without it a rebuild that found everything
    // already intact would never move the watermark forward, and every
    // subsequent startup would redo the same work over the same window.
    bool dayBitmapWatermarkNeedsAdvance() const {
        return maxRecordedDay_ > 0 && (!dayBitmapWatermark_.has_value() || *dayBitmapWatermark_ < maxRecordedDay_);
    }
    // Flush mid-rebuild once this many day bitmaps are dirty: trimDayBitmapCache()
    // cannot evict dirty entries, so an unbounded rebuild pins the whole window
    // in RAM.
    static constexpr size_t kRebuildFlushEveryDirtyKeys = 4096;
    // Rate limit for the "series has no LocalId" warning — one series writing
    // forever with no LocalId must be visible in the log without drowning it.
    uint64_t missingLocalIdDrops_ = 0;
    void warnMissingLocalId(const std::string& measurement, const SeriesId128& seriesId);

    // Measurements whose day history recordDaySpan refused to record in full,
    // with the earliest day it dropped. Per-shard, like the series it describes.
    // Populated on clamp and lazily from KV; a measurement that has never
    // clamped caches as absent, which is safe because the only thing that can
    // introduce a clamp for this shard's series is this shard.
    tsl::robin_map<std::string, std::optional<uint32_t>> clampedHistoryCache_;
    seastar::future<> noteClampedHistory(const std::string& measurement, uint32_t droppedFromDay);
    // The last day the clamp refused to record for this measurement, if any.
    seastar::future<std::optional<uint32_t>> clampedHistoryThrough(const std::string& measurement);
    seastar::future<roaring::Roaring*> getOrLoadDayBitmapForInsert(std::string& cacheKey);
    seastar::future<const roaring::Roaring*> getDayBitmapByKey(const std::string& cacheKey);
    // flushedKeys, when given, receives the cache keys whose dirty flag this
    // call cleared, so a caller whose write then FAILS can put them back.
    void flushDirtyDayBitmaps(IndexWriteBatch& batch, std::vector<std::string>* flushedKeys = nullptr);
    void restoreDirtyDayBitmaps(const std::vector<std::string>& keys, std::optional<uint32_t> previousWatermark);
    seastar::future<roaring::Roaring> buildActiveSeriesBitmap(const std::string& measurement, uint32_t startDay,
                                                              uint32_t endDay);

    // Step 7: Cache eviction — bounded by both entry count and byte budget.
    // Byte budget prevents high-cardinality bitmaps from consuming excessive memory.
    // Cache budgets are a FRACTION OF THIS SHARD'S ARENA, not fixed absolutes.
    //
    // As fixed values these summed to ~690 MB per shard (bitmap 128 + day bitmap
    // 64 + bloom 40 + series 80 + hll 16 + memtables 32 + metadata 48 + discovery
    // 16 + block 8, plus 256 MB of compaction budget), which is ~69% of a 1 GB
    // shard committed before a single point is read. Worse, most were not divided
    // by shard count, so adding shards did not reduce them: per-shard footprint
    // stayed flat while the per-shard arena shrank.
    //
    // Deriving from seastar::memory::stats().total_memory() makes them scale both
    // ways -- small on a 1 GB shard, generous on a 16 GB one -- and keeps the
    // total bounded by construction.
    static size_t indexCacheBudgetBytes();
    static size_t maxBitmapCacheBytes();     // 40% of the index budget
    static size_t maxDayBitmapCacheBytes();  // 20%
    static size_t maxBloomCacheBytes();      // 15%
    static size_t maxHllCacheBytes();        // 10%

    static constexpr size_t MAX_BITMAP_CACHE_ENTRIES = 100000;
    static constexpr size_t MAX_DAY_BITMAP_CACHE_ENTRIES = 50000;
    void trimBitmapCache();
    void trimDayBitmapCache();
    // Step 6: Evict oldest tag values cache entries when over limit
    void trimTagValuesCache();
};

}  // namespace timestar::index
