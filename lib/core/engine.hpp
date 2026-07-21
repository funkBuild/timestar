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
    seastar::future<std::optional<timestar::PushdownResult>> queryAggregated(
        const std::string& seriesKey, const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
        uint64_t aggregationInterval, timestar::AggregationMethod method = timestar::AggregationMethod::AVG,
        bool foldNoInterval = false);

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
