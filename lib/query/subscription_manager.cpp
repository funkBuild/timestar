#include "subscription_manager.hpp"
#include "series_matcher.hpp"
#include "logger.hpp"

#include <algorithm>
#include <regex>
#include <stdexcept>

namespace timestar {

// --- Subscription matching ---

bool Subscription::matches(const std::string& writeMeasurement,
                           const std::map<std::string, std::string>& writeTags,
                           const std::string& writeField) const {
    if (writeMeasurement != measurement) {
        return false;
    }

    // Field filter: empty means all fields match
    if (!fields.empty()) {
        bool fieldFound = false;
        for (const auto& f : fields) {
            if (f == writeField) {
                fieldFound = true;
                break;
            }
        }
        if (!fieldFound) return false;
    }

    // Tag/scope filter: use SeriesMatcher for wildcard/regex support.
    // Pass compiledScopes so pre-compiled std::regex objects are used for
    // wildcard/regex patterns instead of recompiling on every write.
    if (!scopes.empty()) {
        if (!SeriesMatcher::matches(writeTags, scopes, compiledScopes)) {
            return false;
        }
    }

    return true;
}

// --- SubscriptionManager ---

void SubscriptionManager::addSubscription(const Subscription& sub) {
    // Idempotent: skip if already registered (prevents phantom duplicates on reconnect)
    if (_subscriptionsById.count(sub.id)) return;

    bool isLocal = (sub.handlerShard == seastar::this_shard_id());
    if (isLocal && _localSubscriptionCount >= MAX_LOCAL_SUBSCRIPTIONS) {
        throw std::runtime_error("Maximum local subscription limit reached (" +
                                 std::to_string(MAX_LOCAL_SUBSCRIPTIONS) + ")");
    }

    // Pre-compile any wildcard/regex scope patterns so that Subscription::matches()
    // can use the cached std::regex objects instead of recompiling on every write.
    // Throws std::invalid_argument if any pattern is syntactically invalid so that
    // callers can reject the subscription with a meaningful error rather than
    // silently accepting it and producing wrong match results at runtime.
    Subscription compiled = sub;
    for (const auto& [k, pattern] : sub.scopes) {
        if (SeriesMatcher::needsRegexMatch(pattern)) {
            // Let std::invalid_argument (from SeriesMatcher) and std::regex_error
            // propagate — the caller must reject the subscription.
            std::string regexStr = SeriesMatcher::toRegexPattern(pattern);
            compiled.compiledScopes[k] = std::regex(
                regexStr, std::regex::ECMAScript | std::regex::optimize);
        }
    }

    _subscriptionsById[compiled.id] = std::move(compiled);
    _subscriptionsByMeasurement[sub.measurement].push_back(sub.id);
    if (isLocal) ++_localSubscriptionCount;
}

void SubscriptionManager::registerQueue(uint64_t subscriptionId,
                                        seastar::queue<std::shared_ptr<const StreamingBatch>>* queue) {
    _queues[subscriptionId] = queue;
    _sequenceCounters[subscriptionId] = 0;
    _droppedCounters[subscriptionId] = 0;
}

void SubscriptionManager::removeSubscription(uint64_t subscriptionId) {
    auto it = _subscriptionsById.find(subscriptionId);
    if (it == _subscriptionsById.end()) {
        return;
    }

    if (it->second.handlerShard == seastar::this_shard_id()) {
        --_localSubscriptionCount;
    }

    const auto& measurement = it->second.measurement;

    auto measIt = _subscriptionsByMeasurement.find(measurement);
    if (measIt != _subscriptionsByMeasurement.end()) {
        auto& vec = measIt.value();
        std::erase_if(vec, [subscriptionId](uint64_t id) { return id == subscriptionId; });
        if (vec.empty()) {
            _subscriptionsByMeasurement.erase(measIt);
        }
    }

    _subscriptionsById.erase(it);
    _queues.erase(subscriptionId);
    _sequenceCounters.erase(subscriptionId);
    _droppedCounters.erase(subscriptionId);
}

bool SubscriptionManager::hasSubscribers(const std::string& measurement) const {
    auto it = _subscriptionsByMeasurement.find(measurement);
    return it != _subscriptionsByMeasurement.end() && !it->second.empty();
}

void SubscriptionManager::deliverBatch(uint64_t subscriptionId,
                                       std::shared_ptr<const StreamingBatch> batch) {
    auto qIt = _queues.find(subscriptionId);
    if (qIt == _queues.end() || !qIt->second) {
        return;
    }

    auto& seqCounter = _sequenceCounters[subscriptionId];

    // Stamp per-subscriber sequence ID. Note: this deep-copies the points
    // vector for each subscriber. Acceptable for typical workloads but could
    // be optimized with shared_ptr<const vector<...>> for high fan-out.
    auto tagged = std::make_shared<StreamingBatch>(*batch);
    tagged->sequenceId = seqCounter++;

    const auto pointCount = tagged->points.size();
    if (!qIt->second->push(std::move(tagged))) {
        _droppedCounters[subscriptionId] += pointCount;

        // Best-effort: push a drop-notification so the client can observe an
        // explicit drop event rather than just a sequence-ID gap.
        //
        // We do NOT guard with full() before trying: when the data-batch push
        // just failed the queue is at max capacity by definition, so a full()
        // check would always be true and the drop note would never be sent
        // (making that branch permanently dead code).  Instead we attempt the
        // push directly.  If the queue is still full the drop note is silently
        // lost, but _droppedCounters keeps the running total, so the next
        // successful drop note will carry the fully accumulated count.
        auto dropNote = std::make_shared<StreamingBatch>();
        dropNote->isDrop = true;
        dropNote->droppedCount = _droppedCounters[subscriptionId];
        dropNote->sequenceId = seqCounter++;
        dropNote->label = batch->label;
        qIt->second->push(std::move(dropNote));
    }
}

// --- Stats collection ---

std::vector<SubscriptionStats> SubscriptionManager::getStats() const {
    std::vector<SubscriptionStats> stats;
    for (const auto& [id, sub] : _subscriptionsById) {
        // Only report from the handler shard (where the queue lives)
        if (sub.handlerShard != seastar::this_shard_id()) continue;

        SubscriptionStats s;
        s.id = id;
        s.measurement = sub.measurement;
        s.scopes = sub.scopes;
        s.fields = sub.fields;
        s.label = sub.label;
        s.handlerShard = sub.handlerShard;

        auto qIt = _queues.find(id);
        if (qIt != _queues.end() && qIt->second) {
            s.queueDepth = qIt->second->size();
            s.queueCapacity = qIt->second->max_size();
        } else {
            s.queueDepth = 0;
            s.queueCapacity = 0;
        }

        auto dIt = _droppedCounters.find(id);
        s.droppedPoints = (dIt != _droppedCounters.end()) ? dIt->second : 0;

        auto seqIt = _sequenceCounters.find(id);
        s.eventsSent = (seqIt != _sequenceCounters.end()) ? seqIt->second : 0;

        stats.push_back(std::move(s));
    }
    return stats;
}

// --- Variant conversion ---

template <>
std::variant<double, bool, std::string, int64_t>
SubscriptionManager::toVariant(const double& val) { return val; }

template <>
std::variant<double, bool, std::string, int64_t>
SubscriptionManager::toVariant(const bool& val) { return val; }

template <>
std::variant<double, bool, std::string, int64_t>
SubscriptionManager::toVariant(const std::string& val) { return val; }

template <>
std::variant<double, bool, std::string, int64_t>
SubscriptionManager::toVariant(const int64_t& val) { return val; }

// --- buildBatch ---

template <class T>
std::shared_ptr<const StreamingBatch> SubscriptionManager::buildBatch(
    const std::string& measurement,
    const std::map<std::string, std::string>& tags,
    const std::string& field,
    const std::vector<uint64_t>& timestamps,
    const std::vector<T>& values,
    const std::string& label) {

    auto batch = std::make_shared<StreamingBatch>();
    batch->label = label;
    batch->points.reserve(timestamps.size());

    for (size_t i = 0; i < timestamps.size(); ++i) {
        StreamingDataPoint pt;
        pt.measurement = measurement;
        pt.field = field;
        pt.tags = tags;
        pt.timestamp = timestamps[i];
        pt.value = toVariant(values[i]);
        batch->points.push_back(std::move(pt));
    }

    return batch;
}

// --- notifySubscribers ---

template <class T>
std::vector<RemoteDelivery> SubscriptionManager::notifySubscribers(
    const std::string& measurement,
    const std::map<std::string, std::string>& tags,
    const std::string& field,
    const std::vector<uint64_t>& timestamps,
    const std::vector<T>& values) {

    std::vector<RemoteDelivery> remotes;

    auto it = _subscriptionsByMeasurement.find(measurement);
    if (it == _subscriptionsByMeasurement.end()) {
        return remotes;
    }

    unsigned thisShard = seastar::this_shard_id();

    // Build at most one batch per unique label so point data is shared across
    // all matching subscribers (O(unique_labels * M) instead of O(N * M)).
    std::map<std::string, std::shared_ptr<const StreamingBatch>> batchByLabel;

    for (uint64_t subId : it->second) {
        auto subIt = _subscriptionsById.find(subId);
        if (subIt == _subscriptionsById.end()) continue;
        const auto& sub = subIt->second;

        if (!sub.matches(measurement, tags, field)) {
            continue;
        }

        // Lazily build the batch for this label on first match.
        auto& cached = batchByLabel[sub.label];
        if (!cached) {
            cached = buildBatch(measurement, tags, field, timestamps, values, sub.label);
        }

        if (sub.handlerShard == thisShard) {
            // Local: push directly to queue
            deliverBatch(sub.id, cached);
        } else {
            // Remote: return for caller to dispatch (shared_ptr copy is O(1))
            remotes.push_back(RemoteDelivery{sub.id, sub.handlerShard, cached});
        }
    }

    return remotes;
}

// Explicit template instantiations
template std::vector<RemoteDelivery> SubscriptionManager::notifySubscribers<double>(
    const std::string&, const std::map<std::string, std::string>&,
    const std::string&, const std::vector<uint64_t>&, const std::vector<double>&);

template std::vector<RemoteDelivery> SubscriptionManager::notifySubscribers<bool>(
    const std::string&, const std::map<std::string, std::string>&,
    const std::string&, const std::vector<uint64_t>&, const std::vector<bool>&);

template std::vector<RemoteDelivery> SubscriptionManager::notifySubscribers<std::string>(
    const std::string&, const std::map<std::string, std::string>&,
    const std::string&, const std::vector<uint64_t>&, const std::vector<std::string>&);

template std::vector<RemoteDelivery> SubscriptionManager::notifySubscribers<int64_t>(
    const std::string&, const std::map<std::string, std::string>&,
    const std::string&, const std::vector<uint64_t>&, const std::vector<int64_t>&);

} // namespace timestar
