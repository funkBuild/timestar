#ifndef ENGINE_H_INCLUDED
#define ENGINE_H_INCLUDED

#include "block_aggregator.hpp"
#include "http_query_handler.hpp"
#include "leveldb_index.hpp"
#include "query_planner.hpp"
#include "query_result.hpp"
#include "retention_policy.hpp"
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
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>
#include <vector>

class Engine {
private:
    TSMFileManager tsmFileManager;
    WALFileManager walFileManager;
    LevelDBIndex index;
    // TSM conversion runs as background futures per rollover, tracked by WALFileManager's gate

    // Gate to block new inserts during shutdown. Closed early in stop() so
    // in-flight inserts finish but no new ones start.
    seastar::gate _insertGate;

    int shardId;

    // Back-reference to the sharded<Engine> container for cross-shard communication.
    // Used by insert() on non-zero shards to forward metadata indexing to shard 0.
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

    // Synchronous metadata indexing: dispatches metaOps to shard 0's LevelDB index
    // and awaits completion before returning. Guarantees metadata is queryable by the
    // time the write response is sent. Can be called from any shard.
    seastar::future<> indexMetadataSync(std::vector<MetadataOp> metaOps);

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
    seastar::future<std::optional<timestar::PushdownResult>> queryAggregated(const std::string& seriesKey,
                                                                             const SeriesId128& seriesId,
                                                                             uint64_t startTime, uint64_t endTime,
                                                                             uint64_t aggregationInterval);

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
    // Load policies from LevelDB on shard 0 and broadcast to all shards
    seastar::future<> loadAndBroadcastRetentionPolicies();
    // TTL background sweep (dispatched to all shards from shard 0 timer)
    seastar::future<> sweepExpiredFiles();
    // Start the periodic retention sweep timer (shard 0 only, 15min interval)
    void startRetentionSweepTimer();
    // Tombstone-triggered TSM file rewrite: identifies files with >10% estimated dead data
    // and rewrites them at the same tier to reclaim space. Runs on every shard.
    seastar::future<> sweepTombstoneRewrites();

    // Get reference to the index for this shard
    LevelDBIndex& getIndex() { return index; }

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

#endif
