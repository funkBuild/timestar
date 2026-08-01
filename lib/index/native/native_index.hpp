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
#include "storage_layout.hpp"
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
#include <seastar/core/shared_ptr.hh>
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

class NativeIndexLifecycleTestPeer;

// Seastar-native LSM-tree based index backend.
// Uses DMA I/O with no thread-pool crossings.
class NativeIndex : public IndexBackend {
public:
    NativeIndex(timestar::StorageLayout layout, int shardId);
    ~NativeIndex() override;

    // Lifecycle
    seastar::future<> open() override;
    seastar::future<> close() override;
    // Test-only crash boundary: stop and drain NativeIndex-owned background
    // work without performing close()'s final cache/memtable/WAL flush. This
    // must be awaited immediately before destruction. Production owners must
    // use close(); a C++ destructor cannot safely wait for Seastar coroutines.
    seastar::future<> abandonForTesting();

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

    // Per-VShard index extraction (Task 4c): the (seriesId, metadata) pairs whose
    // series belong to `vshard` (virtualShard(seriesId) == vshard), by filtering
    // the SERIES_METADATA entries. Read-only -- it enables extracting one VShard's
    // index for a snapshot without a key-format change; the derived VShard id
    // keeps extraction stable across core counts. Ordered by the index's key
    // order (SeriesId128 bytes).
    seastar::future<std::vector<std::pair<SeriesId128, SeriesMetadata>>> extractVShardSeriesMetadata(uint16_t vshard);

    // Bounded pattern expansion over one VShard's catalog range. This is the
    // storage primitive behind replicated pattern delete discovery: it never
    // scans or returns a foreign VShard and stops as soon as the caller's bound
    // is known to be exceeded.
    seastar::future<std::expected<std::vector<std::string>, SeriesLimitExceeded>> findVShardSeriesKeys(
        uint16_t vshard, const std::string& measurement, const std::map<std::string, std::string>& tagFilters,
        const std::unordered_set<std::string>& fieldFilter, size_t maxSeries, size_t maxEncodedKeyBytes);

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

    // Number of application-cache keys (postings bitmaps, day bitmaps, HLL
    // sketches, measurement blooms) dirtied since the last flush, and the bound
    // the dirty-cache trigger holds it under. Synchronous and O(1) — four set
    // sizes — so a gauge or a test can read it on the reactor. See the
    // dirty-cache flush trigger below (write-scaleout debt D-17).
    size_t dirtyIndexCacheKeys() const;
    static size_t maxDirtyIndexCacheKeys();
    // The companion byte estimate. Read the note on dirtyCacheBytes_ for exactly
    // what it counts -- it is a write-volume estimate, not an arena accountant.
    size_t dirtyIndexCacheBytes() const;
    // Period of the background dirty-cache flush, i.e. the bound on how long
    // index-cache state can live only in RAM once no further writes arrive.
    static std::chrono::milliseconds dirtyIndexCacheFlushInterval();
    // The index WAL's append sequence, which advances only when this index
    // actually writes. Lets a caller assert that an IDLE shard performs no index
    // write at all — the property the periodic flush's strict guard exists to
    // preserve, and one that no cache counter can express. 0 before open().
    uint64_t indexWalSequence() const;

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

    // Schema broadcast: index metadata and return schema changes for broadcast
    seastar::future<SchemaUpdate> indexMetadataBatchWithSchema(const std::vector<MetadataOp>& ops);

    // Apply schema updates from other shards into local caches AND persist
    // them to this shard's KV store, making every shard a complete schema
    // replica (fields/tags blobs via read-modify-write union; tag values via
    // per-value TAG_VALUE_MARKER keys). Idempotent — re-applying a delta
    // (including the origin shard's own) is a harmless union no-op.
    seastar::future<> applySchemaUpdate(SchemaUpdate update);

private:
    friend class NativeIndexLifecycleTestPeer;

    const timestar::StorageLayout layout_;
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
    seastar::future<> stopBackgroundTasks();       // Cancel timers and drain both timer gates

    // --- Dirty application-cache flush (write-scaleout debt D-17) ---
    //
    // The postings-bitmap / day-bitmap / HLL / measurement-bloom caches are
    // mutated WITHOUT writing a KV entry (a day-bitmap add for an already-known
    // series writes nothing to the memtable), yet the only thing that used to
    // drain them was a memtable threshold crossing plus close()/compact(). So a
    // workload that dirties caches without memtable pressure — steady state on a
    // fixed fleet, or a day rollover, which adds real bits to a brand-new day
    // bitmap while creating no series at all — accumulated dirty state with NO
    // bound at all. That is two problems at once:
    //
    //   * MEMORY. The trims deliberately skip dirty entries (evicting one would
    //     discard an unpersisted add), so dirty entries are trim-exempt and the
    //     byte budgets below do not bound them.
    //   * DURABILITY. Everything outstanding is lost on any non-clean exit, and
    //     day-bitmap membership has no repair path on open the way postings do
    //     (the watermark repair rebuilds postings from metadata; the insert
    //     timestamps a day bitmap needs are gone). That is D-3's residual.
    //
    // Two bounds, both routed into the SAME flush body the memtable threshold
    // uses, so there is exactly one way dirty caches reach disk:
    //
    //   * SIZE — checked O(1) on the insert path (`maybeFlushDirtyCaches`),
    //     which is why the accounting is incremental rather than a scan.
    //   * AGE — a periodic timer, because the size bound provably cannot fire
    //     in the steady state that motivated this: a fixed fleet re-touching the
    //     same day bitmap dirties ONE key forever.
    //
    // `flushDirtyCaches()` REQUIRES flushMutex_ to be held: it clears every
    // `dirty` flag, so it must not interleave with another flush (or with the
    // memtable swap/WAL rotate), and the batch it builds is the only remaining
    // copy of those adds until it is applied. `flushDirtyCachesUnderMutex()` is
    // the entry point for callers that do not already hold it.
    seastar::future<> flushDirtyCaches();
    seastar::future<> flushDirtyCachesUnderMutex();
    // Insert-path check. Deliberately NOT a coroutine: the overwhelmingly common
    // answer is "nothing to do", and a coroutine would allocate a frame per call
    // to reach a bare `co_return`.
    seastar::future<> maybeFlushDirtyCaches();
    // True when a flush would actually persist something. The timer consults
    // this rather than flushDirtyCaches()'s own (deliberately looser) guard, so
    // an idle shard with populated-but-clean caches does not rewrite the local-ID
    // counter and watermark, and dirty the memtable, once a second forever.
    bool hasDirtyCacheState() const;
    // noteBitmapChanged() is declared with the bitmap caches below, where
    // BitmapEntry is defined; it is the single place where the `dirty` flag, the
    // dirty-key set and the byte estimate stay consistent.
    void chargeDirtyCacheBytes(size_t bytes);
    // Restore the invariant "no dirty state => nothing for the next flush to
    // write" after a key leaves a dirty set outside a flush. Called by the
    // retention eviction, which is the only such site; without it a stale charge
    // at or above the threshold makes maybeFlushDirtyCaches() take flushMutex_ on
    // every insert, finding nothing to do, until an unrelated flush zeroes it.
    void dischargeDirtyCacheBytesIfClean();

    // WHAT THIS COUNTS, precisely: the serialized bytes the next dirty-cache
    // flush will write. A roaring bitmap's serialized size tracks its container
    // footprint closely, so it also approximates the memory the dirty — and
    // therefore TRIM-EXEMPT — entries hold, which is why the bound exists at all.
    //
    // Charged in two places, and the SECOND is the load-bearing one:
    //   * clean→dirty transition — `bitmap.getSizeInBytes()`, the entry's real
    //     current size. (The cached `approxBytes` is usually equal to it, since a
    //     bitmap can no longer grow while clean, but it is a figure four separate
    //     sites must remember to maintain and one of them already understates it
    //     after a concurrent cold-load merge. Reading the bitmap is authoritative
    //     and costs one container walk per key per flush interval.)
    //   * every subsequent change while the entry STAYS dirty —
    //     kDirtyBitmapAddBytes. Without this the estimate was inert on exactly the
    //     workloads the bound exists for: a day rollover creates an EMPTY day
    //     bitmap, so its transition charge is a few bytes of key no matter how
    //     many million ids then land in it.
    //
    // WHAT IT IS NOT: an accountant for the arena. Three of its four known
    // inaccuracies OVER-count, which only makes the next flush earlier; the fourth
    // under-counts by a bounded amount. Listed so nobody has to rediscover them
    // while chasing a flush that fired early:
    //   * the per-change constant over-states a dense bitmap (4 bytes for an add
    //     that grows a bitmap container by nothing at all);
    //   * a key re-dirtied while a serializer is mid-round is charged its FULL
    //     size again in the next round, though the current round already wrote it;
    //   * a cold load charges the merged KV payload on top of the transition
    //     charge taken against the pre-inserted empty entry;
    //   * the HLL charge is exact (HyperLogLog::SERIALIZED_SIZE), but the bloom
    //     charge is a floor, so a measurement with many distinct tag values is
    //     under-stated — the one under-counting source, bounded by
    //     kDirtyBloomBytesEstimate per measurement.
    // The one place a charge could otherwise become permanently STALE — a dirty
    // entry erased outside a flush, i.e. retention — restores the invariant
    // instead (see dischargeDirtyCacheBytesIfClean).
    //
    // It is also NOT, on its own, what catches a day rollover on a large shard: at
    // a 512 MB budget the threshold is ~51 MB, which one rollover need never
    // reach. The AGE bound covers that case. The two are complements, not
    // redundant spellings of one idea.
    size_t dirtyCacheBytes_ = 0;
    // Resolved once in open(): indexCacheBudgetBytes() reads
    // seastar::memory::stats(), and this is consulted per insert batch. 0 before
    // open(), which also disables the byte trigger on an index that has no
    // caches to dirty yet.
    size_t maxDirtyCacheBytes_ = 0;

    // ONE TENTH of MAX_BITMAP_CACHE_ENTRIES, one fifth of
    // MAX_DAY_BITMAP_CACHE_ENTRIES, so at most 10% of the postings cache and 20%
    // of the day-bitmap cache can be trim-exempt at once and the entry-count trims
    // below keep their meaning. It also caps a single flush round's work, which
    // matters now that the periodic flush can reach it: the serializers offer a
    // preemption point every 64 keys (maybe_yield() suspends only when the task
    // quota is exhausted, so an uncontended flush still runs straight through), so
    // 10K bitmaps is bounded latency rather than a stall.
    static constexpr size_t kMaxDirtyCacheKeys = 10000;
    // Share of the index cache budget that the dirty entries' SERIALIZED size may
    // reach before a flush is forced. 10% is the slice maxHllCacheBytes() already
    // gets: ~51 MB at the 512 MB budget ceiling, ~1.6 MB at the 16 MB floor. Read
    // the note on dirtyCacheBytes_ for what that figure does and does not measure.
    static constexpr size_t kDirtyCacheBudgetPercent = 10;
    // Charged per change to an ALREADY-dirty bitmap. One add grows a roaring array
    // container by 2 bytes and can promote or split a container; 4 over-estimates
    // for dense bitmaps, which is the safe direction (an earlier flush).
    static constexpr size_t kDirtyBitmapAddBytes = 4;
    // Charged for a newly dirtied measurement bloom. BloomFilter::build() sizes
    // from the measurement's distinct tag values, so the real figure ranges from
    // 8 KB to megabytes; 8 KB is its floor, which keeps the estimate a LOWER
    // bound on what the flush writes rather than an optimistic guess.
    static constexpr size_t kDirtyBloomBytesEstimate = 8192;
    // 50x kWalSyncInterval. The WAL sync flushes a user-space buffer and
    // fdatasyncs; this re-serializes every CHANGED bitmap in full, so it is
    // deliberately two orders of magnitude coarser, and the interval is a direct
    // multiplier on write amplification: a continuously-growing day bitmap for a
    // 1M-series measurement serializes ~128 KB per round, i.e. ~26 KB/s at 5 s
    // against ~128 KB/s at 1 s. It costs nothing when nothing changed
    // (hasDirtyCacheState() is four empty-checks plus an integer compare, and an
    // unchanged bitmap is no longer marked dirty at all), and 5 s sits well inside
    // the server's 30 s shutdown budget, so even the timeout `_Exit(1)` path loses
    // at most one interval of day-bitmap membership — the state with no repair
    // path on open — instead of everything since the last memtable flush.
    static constexpr std::chrono::seconds kDirtyCacheFlushInterval{5};
    seastar::timer<> dirtyCacheTimer_;
    seastar::gate dirtyCacheGate_;

    // Step 4: Incremental SSTable refresh — only opens new files and closes removed ones.
    seastar::future<> refreshSSTables();
    // Startup-only reconciliation. The recovered manifest is the authority:
    // validate the whole index namespace, remove only unreferenced protocol
    // outputs, and durably publish their absence before recovery may serve.
    seastar::future<> reconcileSSTableNamespace();
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
        // Approximate serialized size, refreshed at load/flush time. Used by
        // the trim byte budgets so they don't have to walk every bitmap's
        // container list (getSizeInBytes) on every flush.
        size_t approxBytes = 0;
    };
    // HEAP-STABLE CACHE VALUES + AN EVICTION PIN (write-scaleout 5.1). This used to be
    // `robin_map<std::string, BitmapEntry>` -- open addressing, entries BY VALUE -- and
    // that is what made every pointer into it unholdable across a suspension:
    //
    //   * REHASH. Any insert could rehash the map and RELOCATE every entry, so a
    //     `&entry.bitmap` computed before a `co_await` pointed at moved memory after it.
    //   * EVICTION. `trimBitmapCache()` erases clean entries outright, destroying the
    //     `Roaring`, and the flush immediately before it clears the `dirty` flag that was
    //     the entry's only protection from being evicted.
    //
    // Phase 4d fixed the WRITE side by mutating inside the accessor, and left the READ
    // side under a "consume the pointer immediately" contract that is not actually
    // satisfiable -- the invalidation happens BEFORE the caller resumes, so there is no
    // instant at which the caller could have consumed it safely. The contract is retired
    // rather than documented:
    //
    //   * `lw_shared_ptr` values are heap-stable: a rehash moves the 8-byte handle, never
    //     the entry, so no outstanding reference is invalidated by an insert. (lw_, not
    //     std::shared_ptr: the refcount is non-atomic, and an index is shard-local.)
    //   * The handle IS the pin. A reader holding one keeps the entry alive even if the
    //     map drops it, so eviction can never free memory a suspended reader is about to
    //     read. The trims additionally SKIP entries whose handle is not `owned()` (i.e.
    //     someone else holds one), so a pinned entry stays in the map and a reader and a
    //     concurrent writer keep seeing the SAME entry rather than diverging snapshots.
    //
    // Values are never null: create through `ensureEntry()`, which materializes the
    // handle, rather than through `operator[]` (which would insert an empty handle).
    using BitmapHandle = seastar::lw_shared_ptr<BitmapEntry>;
    using BitmapCache = tsl::robin_map<std::string, BitmapHandle>;
    static BitmapHandle ensureEntry(BitmapCache& cache, const std::string& key);
    // Mark an entry dirty after an add that actually CHANGED it, record its key,
    // and charge the D-17 write estimate. See dirtyCacheBytes_ above for what the
    // clean→dirty transition charges and why it is not `approxBytes`.
    void noteBitmapChanged(std::unordered_set<std::string>& dirtyKeys, const std::string& cacheKey, BitmapEntry& entry);

    // In-memory bitmap cache. Key: "measurement\0tagKey\0tagValue"
    // Populated lazily on first access (insert or query), flushed before memtable swap.
    BitmapCache bitmapCache_;
    // Keys of dirty entries (mirrors hllCacheDirty_): flushes iterate this
    // set instead of walking the entire cache (up to 100K entries) per flush.
    std::unordered_set<std::string> bitmapCacheDirtyKeys_;

    // Get or load a bitmap (read-only). Returns an EMPTY handle if not found anywhere.
    // Uses pre-built cache key to avoid double string construction.
    //
    // The handle is a PIN: holding it keeps the entry alive across any number of
    // suspensions, and the trims will not evict an entry while it is held. That retires
    // the old "consume the returned pointer immediately" contract, which was
    // unsatisfiable (the rehash/trim that invalidated the pointer happened BEFORE the
    // caller resumed, so there was no safe instant in which to consume it). Callers may
    // now hold the handle across `co_await`s; the only remaining rule is the ordinary one
    // that a bitmap being MUTATED concurrently must not be iterated, so a caller that
    // yields mid-iteration still copies (see findSeriesByTag).
    seastar::future<BitmapHandle> getPostingsBitmapByKey(const std::string& cacheKey);
    // Load (if cold) the postings bitmap named by `cacheKey` and ADD `localId` to it,
    // returning the resulting cardinality. Marks the entry dirty.
    //
    // IT MUTATES INSIDE ON PURPOSE. This was `getOrLoadBitmapForInsert`, which returned
    // a `roaring::Roaring*` pointing INTO `bitmapCache_` and let the caller write
    // through it. That pointer is not safe to hold for even one reactor task:
    //
    //   * `bitmapCache_` is a tsl::robin_map -- OPEN ADDRESSING -- holding
    //     `BitmapEntry` (and hence `roaring::Roaring`) BY VALUE. Any insert that
    //     rehashes RELOCATES every bitmap; every outstanding pointer dangles.
    //   * `trimBitmapCache()` erases entries outright, destroying the `Roaring`, and
    //     `flushDirtyBitmaps()` clears the `dirty` flag that protects an entry from
    //     eviction (and calls runOptimize/shrinkToFit, which reallocates the
    //     containers) in the same non-suspending region immediately before it.
    //
    // On the cold path the accessor suspends in `kvGet`, so its `co_return &entry.bitmap`
    // and the caller's dereference are in DIFFERENT reactor tasks. Any other insert
    // coroutine on the shard -- or the flush+trim that memory pressure makes routine --
    // runs in between. That is a use-after-free that faults inside
    // `roaring_bitmap_add`, which is exactly the crash reported against these batches
    // (thousands of distinct series, ~40 concurrent, garbage `si_addr` right after
    // seastar's memory-pressure dump; it vanishes when the same BYTES arrive as few
    // series with many timestamps, i.e. when the cache stops rehashing).
    //
    // Returning the cardinality by VALUE removes the last reason a caller needed the
    // pointer. The same discipline `updateTagHLL` already documented for itself.
    seastar::future<uint64_t> addToPostingsBitmapForInsert(std::string& cacheKey, uint32_t localId);
    // Flush dirty bitmaps + batched LOCAL_ID_FORWARD entries into the KV store.
    // Preemptible: a preemption point every 64 keys — maybe_yield() suspends only
    // when the reactor's task quota is exhausted, so this costs a need_preempt()
    // check on an uncontended flush. Safe only because the dirty set is exchanged
    // out first, so a concurrent re-dirty during a suspension survives into the
    // NEXT round instead of being dropped by a trailing clear() (the D-2 shape).
    seastar::future<> flushDirtyBitmaps(IndexWriteBatch& batch);
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
    std::unordered_set<std::string> bloomFullyBuilt_;  // Measurements where bloom KV scan already done
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
    seastar::future<> flushDirtyHLLs(IndexWriteBatch& batch);  // preemptible, see flushDirtyBitmaps
    seastar::future<> flushDirtyMeasurementBlooms(IndexWriteBatch& batch);
    // Step 7: Trim HLL cache after flush — evict non-dirty entries when too large
    void trimHllCache();
    static constexpr size_t MAX_HLL_CACHE_ENTRIES = 1000;
    // Below this many series sharing one (measurement, tagKey, tagValue), the
    // exact roaring bitmap answers cardinality queries directly, so a 16 KB
    // sketch with ~0.8% error would cost memory to be LESS accurate.
    static constexpr uint64_t kTagHllMinCardinality = 10000;

    // --- Phase 3: Time-scoped per-day bitmaps ---
    // Same heap-stable + pinned shape as bitmapCache_ above, for the same reasons; this
    // is the hotter of the two on the insert path (once per series per day per BATCH).
    BitmapCache dayBitmapCache_;
    std::unordered_set<std::string> dayBitmapCacheDirtyKeys_;  // see bitmapCacheDirtyKeys_

    static void buildDayBitmapCacheKey(std::string& out, const std::string& measurement, uint32_t day);
    // Add `localId` to the (measurement, day) bitmap named by `cacheKey`, loading it if
    // cold. Returns true if it was NOT already present (which invalidates day-scoped
    // discovery caches). Mutates INSIDE for the reason spelled out on
    // addToPostingsBitmapForInsert -- this is the path the reported crash was on, being
    // the only one entered once per series per day per BATCH.
    seastar::future<bool> addToDayBitmapForInsert(std::string& cacheKey, uint32_t localId);
    // Read-only accessor; see getPostingsBitmapByKey for what the handle guarantees.
    seastar::future<BitmapHandle> getDayBitmapByKey(const std::string& cacheKey);
    seastar::future<> flushDirtyDayBitmaps(IndexWriteBatch& batch);  // preemptible, see flushDirtyBitmaps
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
