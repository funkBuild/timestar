#pragma once

#include "block_aggregator.hpp"
#include "engine_metrics.hpp"
#include "http_query_handler.hpp"
#include "native_index.hpp"
#include "query_parser.hpp"
#include "query_result.hpp"
#include "retention_policy.hpp"
#include "schema_update.hpp"
#include "series_id.hpp"
#include "shard_query.hpp"
#include "subscription_manager.hpp"
#include "timestar_config.hpp"
#include "timestar_value.hpp"
#include "tsm_compactor.hpp"  // RetentionCompactionContext (compaction retention provider)
#include "tsm_file_manager.hpp"
#include "wal.hpp"
#include "wal_file_manager.hpp"

#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>
#include <vector>

// What the age-driven downsample sweep (Engine::sweepDownsampleRewrites)
// actually did, cumulative per shard.
//
// `seriesEnumerations` and `filesExamined` are the observable proof that the
// no-policy case costs nothing: they are the sweep's ONLY index reads and its
// ONLY per-series work respectively, so both staying at zero is the difference
// between "found no candidates" and "never looked". Without a counter that
// distinction is invisible from outside the process — which is how the original
// retention wiring stayed broken through a green suite.
struct DownsampleSweepStats {
    uint64_t sweeps = 0;              // times the stage was entered (with the stage enabled)
    uint64_t seriesEnumerations = 0;  // index prefix scans issued (the only I/O)
    uint64_t filesExamined = 0;       // files whose sparse index was walked
    uint64_t candidateFiles = 0;      // files that passed the density test
    uint64_t rewrites = 0;            // rewrites started (bounded by the per-sweep cap)
};

class Engine {
private:
    TSMFileManager tsmFileManager;
    WALFileManager walFileManager;
    timestar::index::NativeIndex index;
    timestar::EngineMetrics _metrics;
    // TSM conversion runs as background futures per rollover, tracked by WALFileManager's gate

    // Gate to block new inserts during shutdown. Closed early in stop() so
    // in-flight inserts finish but no new ones start.
    seastar::gate _insertGate;

    unsigned shardId;

    // Back-reference to the sharded<Engine> container for cross-shard communication.
    // Used for schema broadcasts and cross-shard operations; metadata is indexed locally per-shard.
    seastar::sharded<Engine>* shardedRef = nullptr;

    // --- Retention policy cache (all shards) ---
    // Per-shard cache of retention policies, keyed by measurement name.
    // Updated via invoke_on_all when a policy changes. Naturally bounded by
    // measurement count (tens to hundreds). No eviction needed.
    std::unordered_map<std::string, RetentionPolicy> _retentionPolicies;

    // --- Per-series value-type bindings (shard-local) ---
    //
    // A series' id hashes measurement+tags+field only, so nothing about the id
    // distinguishes a float series from a boolean one. This map is the hot-path
    // cache in front of the durable SERIES_VALUE_TYPE (0x18) binding.
    //
    // A MISS HERE NEVER MEANS "NO BINDING". It means "ask the slower oracles"
    // (memory stores, TSM files, then the index). Treating a miss as absence
    // would let a mis-typed write through after a trim and re-open the bug this
    // exists to close.
    std::unordered_map<SeriesId128, TSMValueType, SeriesId128::Hash> _seriesTypeCache;
    static constexpr size_t MAX_SERIES_TYPE_CACHE = 1'000'000;

    // Resolve the type a series is bound to, cheapest oracle first. Returns
    // nullopt only when the series is genuinely unknown everywhere, i.e. this
    // is its first write.
    seastar::future<std::optional<TSMValueType>> resolveSeriesType(const SeriesId128& seriesId);

    // Bind a series to a type (first write) and populate the cache.
    seastar::future<> bindSeriesType(const SeriesId128& seriesId, TSMValueType type);

    // Populate the cache, clearing wholesale when it overflows. Dropping the
    // whole map is safe precisely because a miss re-consults the durable
    // oracles rather than being read as "unbound"; it mirrors how
    // NativeIndex::trimSchemaCaches treats fieldTypeValues_.
    void cacheSeriesType(const SeriesId128& seriesId, TSMValueType type) {
        if (_seriesTypeCache.size() >= MAX_SERIES_TYPE_CACHE)
            _seriesTypeCache.clear();
        _seriesTypeCache[seriesId] = type;
    }

    // --- SeriesId -> measurement cache (shard-local) ---
    //
    // Feeds the compaction retention provider, which needs a measurement name
    // for each series in a merge's INPUT FILES.
    //
    // An entry mapping to a real measurement name is a PERMANENT fact: a
    // SeriesId128 hashes measurement+tags+field, so the mapping is immutable
    // once known and can never go stale.
    //
    // An entry mapping to the EMPTY STRING is the negative sentinel: "this
    // series belongs to no measurement that currently has a policy". That is
    // the one policy-DEPENDENT thing in here, produced by the bulk resolution
    // path below (which learns membership rather than identity), so every
    // mutator of _retentionPolicies drops the cache — see
    // invalidateSeriesMeasurementCache().
    //
    // A miss always re-resolves through the index, which is what makes
    // clearing the whole map on overflow safe (same argument as
    // _seriesTypeCache). 128k entries is ~8 MB worst case.
    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> _seriesMeasurementCache;
    static constexpr size_t MAX_SERIES_MEASUREMENT_CACHE = 128'000;

    void cacheSeriesMeasurement(const SeriesId128& seriesId, const std::string& measurement) {
        if (_seriesMeasurementCache.size() >= MAX_SERIES_MEASUREMENT_CACHE)
            _seriesMeasurementCache.clear();
        _seriesMeasurementCache[seriesId] = measurement;
    }

    // --- SeriesId -> field cache (shard-local) ---
    //
    // Feeds per-field downsample methods (Phase 4). Populated ONLY for series
    // whose measurement declares `fieldMethods`, so a deployment that never
    // uses the feature never allocates an entry.
    //
    // Every entry is a PERMANENT fact for the same reason the measurement cache
    // is: a SeriesId128 hashes measurement+tags+field, so a series' field is
    // immutable. There is deliberately NO negative sentinel here — every series
    // has a field, so "absent" only ever means "not yet looked up", which makes
    // this cache policy-INDEPENDENT and exempt from
    // invalidateSeriesMeasurementCache(). (It is cleared there anyway, only
    // because a policy change is also the moment the set of interesting series
    // changes and the memory is better spent on the new one.)
    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> _seriesFieldCache;

    // At the cap, STOP ADMITTING rather than clear.
    //
    // The measurement cache clears wholesale because a miss there re-resolves
    // through a single 0x0A prefix scan (~40 ms for a whole measurement). A miss
    // HERE costs one sequential 0x05 point lookup PER SERIES — there is no bulk
    // source for a series' field — so a wholesale clear is far more expensive:
    // a single resolution pass larger than the cap would evict everything it had
    // just learned and leave the cache holding one entry, turning a one-off cold
    // cost into the same cost on EVERY subsequent merge, forever.
    //
    // Retaining instead is sound because every entry is an immutable fact (a
    // SeriesId128 hashes measurement+tags+field, so a series' field can never
    // change) and because the one moment the interesting set genuinely shifts —
    // a policy change — clears this cache outright via
    // invalidateSeriesMeasurementCache(). Memory is bounded identically.
    void cacheSeriesField(const SeriesId128& seriesId, const std::string& field) {
        if (_seriesFieldCache.size() >= MAX_SERIES_MEASUREMENT_CACHE && !_seriesFieldCache.contains(seriesId))
            return;
        _seriesFieldCache[seriesId] = field;
    }

    // Drop the cache when the policy set changes: the negative sentinels above
    // are only valid relative to the policy set that produced them.
    //
    // Clearing alone is NOT sufficient, because the resolution that produces
    // sentinels suspends: a provider call that snapshotted the OLD policy set
    // before its index scan writes its sentinels AFTER this clear, repoisoning
    // the cache with answers about a policy set that no longer exists — and
    // nothing invalidates again until the next policy change, so the affected
    // series stay silently exempt from TTL and downsampling forever. The
    // generation counter is what lets a resuming resolution notice that its
    // snapshot was superseded and drop its sentinels; see
    // buildCompactionRetentionContext(). Positive entries need no such guard:
    // a SeriesId128 hashes measurement+tags+field, so an id's measurement is
    // immutable and policy-independent.
    void invalidateSeriesMeasurementCache() {
        _seriesMeasurementCache.clear();
        _seriesFieldCache.clear();
        ++_retentionPolicyGeneration;
    }

    // Bumped by every mutation of _retentionPolicies (see the three cache
    // mutators). Only ever compared for equality.
    uint64_t _retentionPolicyGeneration = 0;

    // Cumulative counters for the age-driven downsample sweep; see
    // getDownsampleSweepStats().
    DownsampleSweepStats _downsampleSweepStats;

    // Above this many unresolved ids, resolve by MEASUREMENT (one 0x0A
    // prefix scan per policy-bearing measurement) instead of by ID (one
    // bloom-filtered point lookup each).
    //
    // Measured on a 100k-series shard: resolving all 100k ids through
    // getSeriesMetadataBatch cost 6.9 s cold (~70 us per bloom-filtered kvGet,
    // issued strictly sequentially — queue-depth-1 latency, not work), and up
    // to 58 s under I/O contention. The 0x0A prefix scan answering the same
    // question for the whole measurement costs 20-230 ms, and the whole
    // provider call end to end drops to ~40 ms. Merges can run every few
    // seconds, so the per-id path is only affordable for small unresolved
    // sets — a tombstone rewrite of one small file, or the trickle of
    // genuinely new series after the cache has warmed.
    static constexpr size_t MAX_UNRESOLVED_FOR_PER_ID_LOOKUP = 4096;

    // --- Streaming subscription manager (per-shard) ---
    timestar::SubscriptionManager _subscriptionManager;

    // Gate that tracks in-flight cross-shard streaming delivery futures.
    // Closed during stop() to ensure no delivery lambda runs against a
    // partially-destroyed Engine after shutdown has begun.
    seastar::gate _streamingGate;

    // --- I/O scheduling groups for fair queue prioritization ---
    // Created once in init(), used by TSM/index readers (query), WAL (write),
    // and compaction (background). When only one class has pending I/O it gets
    // full bandwidth; shares only matter under contention.
    seastar::scheduling_group _queryGroup;
    seastar::scheduling_group _writeGroup;
    seastar::scheduling_group _compactionGroup;
    // WAL->TSM conversion. Deliberately separate from _compactionGroup and at
    // higher shares: draining a memory store to TSM is what releases WAL disk
    // and drains the ingest backlog, so it must outrank tier merges rather
    // than queue behind them.
    seastar::scheduling_group _flushGroup;
    bool _schedulingGroupsCreated = false;

    // --- TTL background sweep infrastructure (shard 0 only) ---
    seastar::gate _retentionGate;
    seastar::timer<seastar::lowres_clock> _retentionTimer;

    seastar::future<> createDirectoryStructure();

    // Shed load (503 + Retry-After via IngestBacklogException) when ANY stage
    // of the pipeline is critically behind. Called at the top of the insert
    // paths, before anything durable happens, so a rejected write leaves no
    // WAL record and no memstore entry. Three independent ceilings, each a
    // different backlog the front of the pipeline cannot see:
    //
    //  1. Retained memory stores -- conversion behind ingest (RAM).
    //  2. Tier-0 file count -- MERGES behind conversion. Each tier-0 file at
    //     high cardinality carries a multi-MB sparse index, so this backlog
    //     is a memory commitment too; a soak that outran merges 3x grew it to
    //     268 files and the pool exhausted into a bad_alloc storm with
    //     shed=0 the whole way down, because admission only watched (1).
    //  3. Free memory itself -- the backstop for every cause not enumerated
    //     above. Once allocation fails INSIDE the pipeline (a conversion or
    //     merge), the failure is data loss and a backlog that can no longer
    //     drain; a shed request is retryable and costs nothing.
    void rejectIfIngestBacklogged() {
        if (walFileManager.isIngestBacklogged()) {
            ++_metrics.inserts_rejected_backlog_total;
            throw timestar::IngestBacklogException("Shard " + std::to_string(shardId) + " ingest backlog: " +
                                                   std::to_string(walFileManager.retainedMemoryStoreCount()) +
                                                   " memory stores awaiting TSM conversion");
        }
        const size_t tier0Files = tsmFileManager.getFileCountInTier(0);
        if (tier0Files >= timestar::config().storage.compaction.tier0_shed_ceiling) {
            ++_metrics.inserts_rejected_backlog_total;
            throw timestar::IngestBacklogException("Shard " + std::to_string(shardId) + " compaction backlog: " +
                                                   std::to_string(tier0Files) + " tier-0 files awaiting merge");
        }
        const size_t freeMem = seastar::memory::stats().free_memory();
        if (freeMem < timestar::config().storage.ingest_min_free_bytes) {
            ++_metrics.inserts_rejected_backlog_total;
            throw timestar::IngestBacklogException("Shard " + std::to_string(shardId) +
                                                   " memory pressure: " + std::to_string(freeMem >> 20) + "MB free");
        }
    }

    // Internal delete implementation without gate acquisition.
    // Callers must already hold _insertGate.
    seastar::future<bool> deleteRangeImpl(std::string seriesKey, uint64_t startTime, uint64_t endTime);

public:
    // Set the back-reference to the sharded<Engine> container.
    // Must be called on every shard after engine.start() and before any inserts.
    void setShardedRef(seastar::sharded<Engine>* ref) { shardedRef = ref; }
    Engine();
    seastar::future<> init();
    seastar::future<> stop();

    // Start the background tier-compaction loop. Called after
    // setIOSchedulingGroups() so merges land in ts_compact rather than main.
    seastar::future<> startBackgroundCompaction();
    seastar::future<> startBackgroundTasks();
    template <class T>
    seastar::future<> insert(TimeStarInsert<T> insertRequest, bool skipMetadataIndexing = false);
    template <class T>
    seastar::future<WALTimingInfo> insertBatch(std::vector<TimeStarInsert<T>> insertRequests);

    // Enforce each request's series type binding, converting losslessly where
    // possible. Returns the subset whose type matches T; anything bound to a
    // different type is converted and re-inserted through insertBatch<U>.
    // Throws std::invalid_argument (-> HTTP 400) when a value cannot be
    // converted without loss.
    //
    // Public only so tests can drive it directly; callers should use
    // insertBatch, which invokes it.
    template <class T>
    seastar::future<std::vector<TimeStarInsert<T>>> enforceSeriesTypes(std::vector<TimeStarInsert<T>> requests);
    template <class T>
    seastar::future<SeriesId128> indexMetadata(TimeStarInsert<T> insertRequest);
    seastar::future<> indexMetadataBatch(const std::vector<MetadataOp>& ops);

    // Synchronous metadata indexing: dispatches metaOps to each owning shard's index
    // and broadcasts schema changes to all shards. Guarantees metadata is queryable
    // by the time the write response is sent. Can be called from any shard.
    seastar::future<> indexMetadataSync(std::vector<MetadataOp> metaOps);

    // Broadcast a SchemaUpdate to all shards' NativeIndex caches.
    seastar::future<> broadcastSchemaUpdate(timestar::index::SchemaUpdate update);

    seastar::future<> rolloverMemoryStore();
    // Returns std::nullopt if series doesn't exist (rather than throwing)
    seastar::future<std::optional<VariantQueryResult>> query(std::string series, uint64_t startTime, uint64_t endTime);
    // Overload accepting pre-computed SeriesId128 to avoid redundant SHA1
    seastar::future<std::optional<VariantQueryResult>> query(std::string series, const SeriesId128& seriesId,
                                                             uint64_t startTime, uint64_t endTime);

    // Index-based query methods
    seastar::future<VariantQueryResult> queryBySeries(std::string measurement, std::map<std::string, std::string> tags,
                                                      std::string field, uint64_t startTime, uint64_t endTime);

    // Metadata queries
    seastar::future<std::vector<std::string>> getAllMeasurements();
    seastar::future<std::set<std::string>> getMeasurementFields(const std::string& measurement);
    seastar::future<std::set<std::string>> getMeasurementTags(const std::string& measurement);
    seastar::future<std::set<std::string>> getTagValues(const std::string& measurement, const std::string& tagKey);

    // Execute a local query on this shard
    seastar::future<std::vector<timestar::SeriesResult>> executeLocalQuery(const timestar::ShardQuery& shardQuery);

    // Prefetch TSM index entries for a batch of series (warms cache before per-series queries)
    seastar::future<> prefetchSeriesIndices(const std::vector<SeriesId128>& seriesIds);

    // Pushdown aggregation: returns aggregated state directly from TSM blocks,
    // bypassing full point materialisation. Returns nullopt when pushdown is
    // inapplicable (non-float, memory store data, cross-file overlap).
    //
    // foldNoInterval selects the aggregationInterval == 0 result shape for
    // streamable non-LATEST/FIRST methods: true collapses everything into a
    // single AggregationState; false returns raw sorted (timestamp, value)
    // vectors for per-timestamp results.  Must be derived from the query
    // alone, never from data placement (see QueryRunner::queryTsmAggregated).
    //
    // Defaults to FALSE because collapsing a range that the caller did not ask
    // to collapse violates the canonical shape rules (CLAUDE.md "Aggregation
    // Result Shape"): without an aggregationInterval every distinct timestamp
    // must survive, and LATEST/FIRST — the only methods that collapse by
    // definition — do so inside the runner regardless of this flag.  Both
    // production call sites pass false explicitly; no caller should need true.
    //
    // boolLatestAsNumeric admits BOOLEAN series to the bucketed LATEST fast
    // path, folding true/false as 1.0/0.0 (LATEST selects, never computes, so
    // the caller's conversion back to bool is exact).  Honoured only for
    // method == LATEST with aggregationInterval > 0; see
    // QueryRunner::queryTsmAggregated.
    seastar::future<std::optional<timestar::PushdownResult>> queryAggregated(
        const std::string& seriesKey, const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
        uint64_t aggregationInterval, timestar::AggregationMethod method = timestar::AggregationMethod::AVG,
        bool foldNoInterval = false, bool boolLatestAsNumeric = false);

    // No-I/O probe of a series' value type from this shard's memory stores and
    // TSM sparse indexes.  nullopt when the series is unknown on this shard.
    std::optional<TSMValueType> localSeriesValueType(const SeriesId128& seriesId);

    // Batch LATEST/FIRST: resolve latest (or first) value for multiple series
    // in a single pass over TSM files and memory stores.  Avoids per-series
    // file snapshot, sort, and coroutine overhead.
    struct BatchLatestEntry {
        SeriesId128 seriesId;
        uint64_t timestamp = 0;
        double value = 0.0;
        bool resolved = false;
    };
    seastar::future<> batchLatest(std::vector<BatchLatestEntry>& entries, uint64_t startTime, uint64_t endTime,
                                  bool wantFirst = false);

    // --- Retention policy management ---
    // Update a single policy in this shard's cache (called via invoke_on_all)
    void updateRetentionPolicyCache(const RetentionPolicy& policy);
    // Remove a policy from this shard's cache (called via invoke_on_all)
    void removeRetentionPolicyCache(const std::string& measurement);
    // Replace the entire cache (called during startup broadcast)
    void setRetentionPolicies(std::unordered_map<std::string, RetentionPolicy> policies);
    // Get the retention policy for a measurement (local cache lookup, no I/O)
    std::optional<RetentionPolicy> getRetentionPolicy(const std::string& measurement) const;
    // Load policies from NativeIndex on shard 0 and broadcast to all shards
    seastar::future<> loadAndBroadcastRetentionPolicies();
    // TTL background sweep (dispatched to all shards from shard 0 timer)
    seastar::future<> sweepExpiredFiles();
    // Start the periodic retention sweep timer (shard 0 only, 15min interval)
    void startRetentionSweepTimer();
    // Tombstone-triggered TSM file rewrite: identifies files with >10% estimated dead data
    // and rewrites them at the same tier to reclaim space. Runs on every shard.
    seastar::future<> sweepTombstoneRewrites();

    // Third stage of the retention sweep: AGE-DRIVEN DOWNSAMPLING.
    //
    // A tier merge needs files_per_merge files to accumulate, so a series that
    // stops receiving writes is never re-compacted and never folds, however old
    // its data gets. This finds files holding aged, still-too-dense data and
    // rewrites them in place (single file, same tier) so the Phase 1 retention
    // provider applies the cascade.
    //
    // The candidate test performs NO I/O — it reads only the resident sparse
    // index — and the sweep is self-limiting with no persisted watermark: an
    // already-folded series' stored density sits at ~1 point per bucket and
    // fails the hysteresis check. Runs on every shard.
    //
    // Public so tests can drive one sweep deterministically instead of waiting
    // out the 15-minute timer.
    seastar::future<> sweepDownsampleRewrites();

    // What the age-driven downsample stage actually did, cumulative per shard.
    const DownsampleSweepStats& getDownsampleSweepStats() const { return _downsampleSweepStats; }

    // Retention context provider for the compactor, installed in init().
    //
    // Public so tests can drive it directly — the wiring it replaces went
    // unnoticed for exactly as long as it did because every retention test
    // entered BELOW this layer.
    //
    // Returns empty maps without touching the index when no policy on this
    // shard carries a TTL or a downsample clause (the common case). Throws on
    // an index failure, which fails the compaction rather than silently
    // compacting without retention.
    seastar::future<RetentionCompactionContext> buildCompactionRetentionContext(
        const std::vector<SeriesId128>& seriesIds);

    // Second half of the context: seriesId -> FIELD, for the series whose
    // measurement declares per-field downsample methods. Returns immediately
    // when no policy does — see the cost contract on the definition.
    //
    // Public for the same reason the provider is: tests must be able to observe
    // that the default path performs no field resolution.
    seastar::future<RetentionCompactionContext> attachFieldOverrides(RetentionCompactionContext ctx);

    // Entries currently resolved in the seriesId -> field cache. Zero on every
    // shard whose policies name no `fieldMethods`, which is what makes the
    // "costs nothing by default" claim observable rather than asserted.
    size_t seriesFieldCacheSize() const { return _seriesFieldCache.size(); }

    // Does any policy on this shard carry a TTL or a usable downsample clause?
    // Injected into the compactor alongside the provider so a merge on a shard
    // with no (effective) policy skips even enumerating its series ids.
    bool hasActionableRetentionPolicy() const;

    // Entries currently resolved in the seriesId -> measurement cache. Exposed
    // so tests can observe that the no-policy fast path performed no index
    // resolution at all (a resolution always populates this cache).
    size_t seriesMeasurementCacheSize() const { return _seriesMeasurementCache.size(); }

    // Get reference to the index for this shard
    timestar::index::NativeIndex& getIndex() { return index; }

    // Per-shard Prometheus metrics counters
    timestar::EngineMetrics& metrics() { return _metrics; }

    // Gauge accessors for metrics
    size_t getTSMFileCount() const { return tsmFileManager.getSequencedTsmFiles().size(); }
    uint64_t getCompletedCompactions() const { return tsmFileManager.getCompletedCompactions(); }
    size_t getRetainedMemoryStoreCount() const { return walFileManager.retainedMemoryStoreCount(); }

    // Read-only view of compaction placement. Exposed so tests can assert that
    // background work actually landed in its scheduling group -- the original
    // bug was invisible precisely because nothing could observe this.
    const TSMFileManager& getTSMFileManager() const { return tsmFileManager; }
    // Mutable access, for tests that must drive a merge deterministically
    // (compactOneTier) instead of waiting on the background loop.
    TSMFileManager& getTSMFileManager() { return tsmFileManager; }
    const timestar::EngineMetrics& getMetrics() const { return _metrics; }

    // Compaction health. A tier that can never merge is otherwise invisible from
    // outside the process: the original production incident ran for 15 minutes
    // with /health reporting "healthy" while the tier grew without bound and the
    // server had already started rejecting writes.
    uint64_t getCompactionFailures() const { return tsmFileManager.getTotalCompactionFailures(); }
    int getDeepestBackloggedTier() const { return tsmFileManager.getDeepestBackloggedTier(); }
    uint64_t getMaxConsecutiveCompactionFailures() const {
        uint64_t worst = 0;
        for (uint64_t tier = 0; tier < TSMFileManager::maxTiers(); ++tier) {
            worst = std::max(worst, tsmFileManager.getConsecutiveFailures(tier));
        }
        return worst;
    }

    // Set I/O scheduling groups (called from main after create_scheduling_group).
    // create_scheduling_group is a global operation, so groups must be created
    // once from any shard and then distributed via invoke_on_all.
    //
    // The groups MUST be forwarded to tsmFileManager here and not only from
    // init(). The server calls init() BEFORE creating the groups, so the
    // init()-side guard never fired in production: _compactionGroupSet stayed
    // false for the whole process lifetime, every with_scheduling_group site
    // silently took its inline fallback, and all compaction ran in `main`
    // alongside writes. A 76s tier merge was 76s of unavailability while
    // ts_compact sat at zero runtime. Forwarding from both sides makes the
    // handshake order-independent.
    void setIOSchedulingGroups(seastar::scheduling_group query, seastar::scheduling_group write,
                               seastar::scheduling_group compaction, seastar::scheduling_group flush) {
        _queryGroup = query;
        _writeGroup = write;
        _compactionGroup = compaction;
        _flushGroup = flush;
        _schedulingGroupsCreated = true;
        tsmFileManager.setCompactionGroup(compaction);
        tsmFileManager.setFlushGroup(flush);
    }

    // Get reference to the subscription manager for this shard
    timestar::SubscriptionManager& getSubscriptionManager() { return _subscriptionManager; }

    // Delete operations
    seastar::future<bool> deleteRange(std::string seriesKey, uint64_t startTime, uint64_t endTime);

    seastar::future<bool> deleteRangeBySeries(std::string measurement, std::map<std::string, std::string> tags,
                                              std::string field, uint64_t startTime, uint64_t endTime);

    // Flexible deletion interface
    struct DeleteRequest {
        std::string measurement;
        std::map<std::string, std::string> tags;  // Optional: empty means all tags
        std::vector<std::string> fields;          // Optional: empty means all fields
        uint64_t startTime;
        uint64_t endTime;
    };

    struct DeleteResult {
        uint64_t seriesDeleted = 0;
        uint64_t pointsDeleted = 0;              // Estimated
        std::vector<std::string> deletedSeries;  // Series keys that were deleted
    };

    seastar::future<DeleteResult> deleteByPattern(const DeleteRequest& request);

    std::string basePath();
};
