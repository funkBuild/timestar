#include "query_planner.hpp"
#include "series_key.hpp"
#include <seastar/core/seastar.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>
#include <algorithm>
#include <functional>
#include <unordered_set>

namespace timestar {

seastar::future<QueryPlan> QueryPlanner::createPlan(
    const QueryRequest& request,
    seastar::sharded<LevelDBIndex>* indexSharded) {
    
    QueryPlan plan;
    plan.aggregation = request.aggregation;
    plan.aggregationInterval = request.aggregationInterval;
    plan.groupByTags = request.groupByTags;
    
    // Find all matching series IDs across all shards
    auto shardBuckets = co_await findMatchingSeriesIds(request, indexSharded);

    // Create shard queries
    for (unsigned shardId = 0; shardId < shardBuckets.size(); ++shardId) {
        const auto& seriesIds = shardBuckets[shardId];
        if (!seriesIds.empty()) {
            ShardQuery sq;
            sq.shardId = shardId;
            sq.seriesIds = seriesIds;
            sq.startTime = request.startTime;
            sq.endTime = request.endTime;
            sq.requiresAllSeries = false;

            // Set fields to query
            if (request.requestsAllFields()) {
                // Will query all fields for each series
                sq.fields.clear();  // Empty means all fields
            } else {
                sq.fields.insert(request.fields.begin(), request.fields.end());
            }

            plan.shardQueries.push_back(sq);
            plan.estimatedSeriesCount += seriesIds.size();
        }
    }
    
    plan.requiresMerging = plan.shardQueries.size() > 1;
    
    co_return plan;
}

QueryPlan QueryPlanner::createPlanSync(
    const QueryRequest& request,
    LevelDBIndex* index) {
    
    QueryPlan plan;
    plan.aggregation = request.aggregation;
    plan.aggregationInterval = request.aggregationInterval;
    plan.groupByTags = request.groupByTags;
    
    // Find matching series IDs (synchronous version for testing)
    std::vector<std::vector<SeriesId128>> shardBuckets;

    // For testing, simulate finding series without actual index operations
    // This is just for unit testing the planner logic
    if (!request.measurement.empty()) {
        // Use the more detailed mock implementation
        shardBuckets = findMatchingSeriesIdsSync(request, index);
    }

    // Create shard queries
    for (unsigned shardId = 0; shardId < shardBuckets.size(); ++shardId) {
        const auto& seriesIds = shardBuckets[shardId];
        if (!seriesIds.empty()) {
            ShardQuery sq;
            sq.shardId = shardId;
            sq.seriesIds = seriesIds;
            sq.startTime = request.startTime;
            sq.endTime = request.endTime;
            sq.requiresAllSeries = false;

            // Set fields to query
            if (request.requestsAllFields()) {
                sq.fields.clear();  // Empty means all fields
            } else {
                sq.fields.insert(request.fields.begin(), request.fields.end());
            }

            plan.shardQueries.push_back(sq);
            plan.estimatedSeriesCount += seriesIds.size();
        }
    }
    
    plan.requiresMerging = plan.shardQueries.size() > 1;
    
    return plan;
}

seastar::future<std::vector<std::vector<SeriesId128>>>
QueryPlanner::findMatchingSeriesIds(
    const QueryRequest& request,
    seastar::sharded<LevelDBIndex>* indexSharded) {

    // Metadata is centralized on shard 0; query only shard 0 for series discovery,
    // then distribute results to the appropriate data shards by SeriesId128 hash.
    auto allSeriesIds = co_await indexSharded->invoke_on(0,
        [request](LevelDBIndex& index) -> seastar::future<std::vector<SeriesId128>> {
            std::unordered_set<std::string> fieldFilter;
            if (!request.requestsAllFields()) {
                fieldFilter.insert(request.fields.begin(), request.fields.end());
            }

            auto findResult = co_await index.findSeriesWithMetadata(
                request.measurement, request.scopes, fieldFilter);

            // If limit was exceeded, throw so the caller can report the error
            if (!findResult.has_value()) {
                throw std::runtime_error(
                    "Query matches too many series for measurement '" +
                    request.measurement + "'. Narrow your query with more specific tag filters.");
            }

            auto& seriesWithMeta = findResult.value();
            std::vector<SeriesId128> seriesIds;
            seriesIds.reserve(seriesWithMeta.size());
            for (const auto& swm : seriesWithMeta) {
                seriesIds.push_back(swm.seriesId);
            }

            co_return seriesIds;
        });

    // Distribute series to their data shards by SeriesId128 hash
    unsigned shardCount = seastar::smp::count;
    if (shardCount == 0) shardCount = 1;  // Handle test environment

    std::vector<std::vector<SeriesId128>> shardBuckets(shardCount);

    for (const auto& seriesId : allSeriesIds) {
        size_t hash = SeriesId128::Hash{}(seriesId);
        unsigned shardId = hash % shardCount;
        shardBuckets[shardId].push_back(seriesId);
    }

    co_return shardBuckets;
}

std::vector<std::vector<SeriesId128>>
QueryPlanner::findMatchingSeriesIdsSync(
    const QueryRequest& request,
    LevelDBIndex* index) {

    unsigned shardCount = seastar::smp::count;
    if (shardCount == 0) shardCount = 1;  // Handle test environment

    std::vector<std::vector<SeriesId128>> shardBuckets(shardCount);

    // This synchronous version is only used by unit tests (createPlanSync).
    // Production code uses the async findMatchingSeriesIds() which queries
    // the real LevelDB index via invoke_on(0).  The index parameter is
    // accepted but unused because LevelDB requires async I/O.
    (void)index;

    if (!request.measurement.empty()) {
        std::set<std::string> queryFields;

        if (request.requestsAllFields()) {
            queryFields = {"value", "field1", "field2"};
        } else {
            queryFields.insert(request.fields.begin(), request.fields.end());
        }

        for (const auto& field : queryFields) {
            unsigned shardId = calculateShardForSeries(
                request.measurement, request.scopes, field);

            std::string mockSeriesKey = request.measurement;
            for (const auto& [tagKey, tagValue] : request.scopes) {
                mockSeriesKey += "," + tagKey + "=" + tagValue;
            }
            mockSeriesKey += " " + field;
            SeriesId128 mockSeriesId = SeriesId128::fromSeriesKey(mockSeriesKey);
            shardBuckets[shardId].push_back(mockSeriesId);
        }
    }

    return shardBuckets;
}

std::vector<std::vector<SeriesId128>> QueryPlanner::mapSeriesToShards(
    const std::vector<SeriesId128>& seriesIds,
    const std::string& measurement,
    const std::map<std::string, std::string>& tags,
    const std::vector<std::string>& fields) {

    unsigned shardCount = seastar::smp::count;
    if (shardCount == 0) shardCount = 1;  // Handle test environment

    std::vector<std::vector<SeriesId128>> shardBuckets(shardCount);

    // For each series ID, determine its shard
    for (const SeriesId128& seriesId : seriesIds) {
        // Use the hash function from SeriesId128 for distribution
        size_t hash = std::hash<SeriesId128>{}(seriesId);
        unsigned shardId = hash % shardCount;
        shardBuckets[shardId].push_back(seriesId);
    }

    return shardBuckets;
}

unsigned QueryPlanner::calculateShardForSeries(
    const std::string& measurement,
    const std::map<std::string, std::string>& tags,
    const std::string& field) {
    
    // Build the series key and hash it to determine shard using SeriesId128
    std::string seriesKey = buildSeriesKeyForSharding(measurement, tags, field);
    
    unsigned shardCount = seastar::smp::count;
    if (shardCount == 0) shardCount = 1;  // Handle test environment
    
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
    size_t hash = SeriesId128::Hash{}(seriesId);
    return hash % shardCount;
}

std::string QueryPlanner::buildSeriesKeyForSharding(
    const std::string& measurement,
    const std::map<std::string, std::string>& tags,
    const std::string& field) {
    return buildSeriesKey(measurement, tags, field);
}

bool QueryPlanner::requiresAllShards(const QueryRequest& request) {
    // If no specific tags are provided, we need to query all shards
    // Also if using wildcards or regex, we may need all shards
    
    if (request.scopes.empty()) {
        return true;
    }
    
    // Check for wildcards or regex in scopes
    for (const auto& [key, value] : request.scopes) {
        if (value.find('*') != std::string::npos ||
            value.find('?') != std::string::npos ||
            (!value.empty() && (value[0] == '/' || value[0] == '~'))) {
            return true;
        }
    }
    
    return false;
}

} // namespace timestar