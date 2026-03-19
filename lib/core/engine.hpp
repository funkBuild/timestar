#pragma once

#include "block_aggregator.hpp"
#include "engine_metrics.hpp"
#include "http_query_handler.hpp"
#include "native_index.hpp"
#include "query_parser.hpp"
#include "shard_query.hpp"
#include "query_result.hpp"
#include "retention_policy.hpp"
#include "schema_update.hpp"
#include "series_id.hpp"
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

    int shardId;

    // Back-reference to the sharded<Engine> container for cross-shard communication.
    // Used for schema broadcasts and cross-shard operations; metadata is indexed locally per-shard.
    seastar::sharded<Engine>* shardedRef = nullptr;

    // --- Retention policy cache (all shards) ---
    // Per-shard cache of retention policies, keyed by measurement name.
    // Updated via invoke_on_all when a policy changes. Naturally bounded by
    // measurement count (tens to hundreds). No eviction needed.
    std::unordered_map<std::string, RetentionPolicy> _retentionPolicies;

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
    bool _schedulingGroupsCreated = false;

    // --- TTL background sweep infrastructure (shard 0 only) ---
    seastar::gate _retentionGate;
    seastar::timer<seastar::lowres_clock> _retentionTimer;

    seastar::future<> createDirectoryStructure();

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
    seastar::future<> startBackgroundTasks();
    template <class T>
    seastar::future<> insert(TimeStarInsert<T> insertRequest, bool skipMetadataIndexing = false);
    template <class T>
    seastar::future<WALTimingInfo> insertBatch(std::vector<TimeStarInsert<T>> insertRequests);
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
    seastar::future<std::optional<timestar::PushdownResult>> queryAggregated(
        const std::string& seriesKey, const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
        uint64_t aggregationInterval, timestar::AggregationMethod method = timestar::AggregationMethod::AVG);

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
    // Get all retention policies (local cache)
    const std::unordered_map<std::string, RetentionPolicy>& getRetentionPolicies() const { return _retentionPolicies; }
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

    // I/O scheduling groups — use with seastar::with_scheduling_group() to
    // prioritize query I/O over background compaction.
    // Returns default_scheduling_group() if groups haven't been set (e.g., tests).
    seastar::scheduling_group queryGroup() const {
        return _schedulingGroupsCreated ? _queryGroup : seastar::default_scheduling_group();
    }
    seastar::scheduling_group compactionGroup() const {
        return _schedulingGroupsCreated ? _compactionGroup : seastar::default_scheduling_group();
    }

    // Set I/O scheduling groups (called from main after create_scheduling_group).
    // create_scheduling_group is a global operation, so groups must be created
    // once from any shard and then distributed via invoke_on_all.
    void setIOSchedulingGroups(seastar::scheduling_group query, seastar::scheduling_group write,
                               seastar::scheduling_group compaction) {
        _queryGroup = query;
        _writeGroup = write;
        _compactionGroup = compaction;
        _schedulingGroupsCreated = true;
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
