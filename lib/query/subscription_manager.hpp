#pragma once

#include "query_parser.hpp"
#include "series_matcher.hpp"

#include <tsl/robin_map.h>

#include <map>
#include <memory>
#include <regex>
#include <seastar/core/queue.hh>
#include <seastar/core/sharded.hh>
#include <string>
#include <variant>
#include <vector>

namespace timestar {

using TagMap = std::map<std::string, std::string>;

inline std::shared_ptr<const TagMap> makeTags(TagMap tags) {
    return std::make_shared<const TagMap>(std::move(tags));
}

// A single data point notification for streaming to a subscriber.
struct StreamingDataPoint {
    std::string measurement;
    std::string field;
    std::shared_ptr<const TagMap> tags = makeTags({});  // Shared across points in a batch (avoids N copies)
    uint64_t timestamp = 0;
    std::variant<double, bool, std::string, int64_t> value;
};

// A batch of data points that matched a subscription.
struct StreamingBatch {
    std::vector<StreamingDataPoint> points;
    uint64_t sequenceId = 0;
    std::string label;          // Query label for multi-query subscriptions (empty = single-query)
    bool isDrop = false;        // true for drop-notification batches (no points)
    uint64_t droppedCount = 0;  // number of points dropped (valid when isDrop=true)
};

// Per-subscription filter criteria. Stored on EVERY shard for local matching.
struct Subscription {
    uint64_t id = 0;
    std::string measurement;
    std::map<std::string, std::string> scopes;  // Tag filters (AND)
    std::vector<std::string> fields;            // Field filters (empty = all)
    AggregationMethod aggregation = AggregationMethod::AVG;
    unsigned handlerShard = 0;  // Shard that owns the output queue
    std::string label;          // Query label for multi-query subscriptions

    // Pre-compiled regex patterns for scope values that use wildcard/regex syntax.
    // Populated by SubscriptionManager::addSubscription() so that matches() avoids
    // recompiling the same std::regex on every write in the hot path.
    // Key matches a key in scopes; only entries that need regex matching are present.
    // Declared mutable so it can be populated after construction (e.g. on copy).
    mutable std::map<std::string, std::regex> compiledScopes;

    // Check if a write matches this subscription's filters.
    bool matches(const std::string& writeMeasurement, const std::map<std::string, std::string>& writeTags,
                 const std::string& writeField) const;
};

// Returned by notifySubscribers for batches that need cross-shard delivery.
struct RemoteDelivery {
    uint64_t subscriptionId;
    unsigned targetShard;
    std::shared_ptr<const StreamingBatch> batch;
};

// Stats for a single subscription (returned by monitoring endpoint).
struct SubscriptionStats {
    uint64_t id;
    std::string measurement;
    std::map<std::string, std::string> scopes;
    std::vector<std::string> fields;
    std::string label;
    unsigned handlerShard;
    uint64_t queueDepth;
    uint64_t queueCapacity;
    uint64_t droppedPoints;
    uint64_t eventsSent;
};

class SubscriptionManager {
public:
    SubscriptionManager() = default;

    // Register a subscription on this shard.
    void addSubscription(const Subscription& sub);

    // Register a local output queue for a subscription (handler shard only).
    void registerQueue(uint64_t subscriptionId, seastar::queue<std::shared_ptr<const StreamingBatch>>* queue);

    // Remove a subscription from this shard.
    void removeSubscription(uint64_t subscriptionId);

    // O(1) check: are there any subscribers for this measurement?
    bool hasSubscribers(const std::string& measurement) const;

    // Notify matching subscribers with new data. For subscriptions whose
    // handlerShard == this shard, pushes directly to the queue. For remote
    // subscriptions, adds to the returned vector for the caller to dispatch.
    template <class T>
    std::vector<RemoteDelivery> notifySubscribers(const std::string& measurement,
                                                  const std::map<std::string, std::string>& tags,
                                                  const std::string& field, const std::vector<uint64_t>& timestamps,
                                                  const std::vector<T>& values);

    // Deliver a batch to a local subscription's queue. Called via invoke_on()
    // from remote shards.
    void deliverBatch(uint64_t subscriptionId, std::shared_ptr<const StreamingBatch> batch);

    // Generate a globally unique subscription ID (upper 16 bits = shard ID).
    // Must be called on the handler shard before registering across shards.
    uint64_t allocateId() { return (uint64_t(seastar::this_shard_id()) << 48) | (++_nextSubscriptionId); }

    // Total subscription entries on this shard (includes filter copies for remote handler shards).
    // Use localSubscriptionCount() for capacity enforcement.
    size_t subscriptionCount() const { return _subscriptionsById.size(); }

    // Count subscriptions managed by this shard (handler-shard role only).
    // Use this for capacity limit checks — subscriptionCount() overcounts
    // because non-handler shards also store filter entries for remote subscriptions.
    size_t localSubscriptionCount() const { return _localSubscriptionCount; }

    // Collect stats for all subscriptions on this shard (handler shard only has queue info).
    std::vector<SubscriptionStats> getStats() const;

    seastar::future<> stop() { return seastar::make_ready_future<>(); }

private:
    tsl::robin_map<std::string, std::vector<uint64_t>> _subscriptionsByMeasurement;
    tsl::robin_map<uint64_t, Subscription> _subscriptionsById;

    // Handler-shard-only: output queues, sequence counters, drop counters
    tsl::robin_map<uint64_t, seastar::queue<std::shared_ptr<const StreamingBatch>>*> _queues;
    tsl::robin_map<uint64_t, uint64_t> _sequenceCounters;
    tsl::robin_map<uint64_t, uint64_t> _droppedCounters;

    static constexpr size_t MAX_LOCAL_SUBSCRIPTIONS = 10'000;
    uint64_t _nextSubscriptionId = 0;
    size_t _localSubscriptionCount = 0;

    template <class T>
    static std::variant<double, bool, std::string, int64_t> toVariant(const T& val);

    template <class T>
    static std::shared_ptr<const StreamingBatch> buildBatch(const std::string& measurement,
                                                            const std::map<std::string, std::string>& tags,
                                                            const std::string& field,
                                                            const std::vector<uint64_t>& timestamps,
                                                            const std::vector<T>& values, const std::string& label);
};

}  // namespace timestar
