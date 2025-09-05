#include "query_planner.hpp"
#include <seastar/core/seastar.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>
#include <algorithm>
#include <functional>

namespace tsdb {

seastar::future<QueryPlan> QueryPlanner::createPlan(
    const QueryRequest& request,
    seastar::sharded<LevelDBIndex>* indexSharded) {
    
    QueryPlan plan;
    plan.aggregation = request.aggregation;
    plan.aggregationInterval = request.aggregationInterval;
    plan.groupByTags = request.groupByTags;
    
    // Find all matching series IDs across all shards
    auto seriesByShardMap = co_await findMatchingSeriesIds(request, indexSharded);
    
    // Create shard queries
    for (const auto& [shardId, seriesIds] : seriesByShardMap) {
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
    std::map<unsigned, std::vector<uint64_t>> seriesByShardMap;
    
    // For testing, simulate finding series without actual index operations
    // This is just for unit testing the planner logic
    if (!request.measurement.empty()) {
        // Use the more detailed mock implementation
        seriesByShardMap = findMatchingSeriesIdsSync(request, index);
    }
    
    // Create shard queries
    for (const auto& [shardId, seriesIds] : seriesByShardMap) {
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

seastar::future<std::map<unsigned, std::vector<uint64_t>>> 
QueryPlanner::findMatchingSeriesIds(
    const QueryRequest& request,
    seastar::sharded<LevelDBIndex>* indexSharded) {
    
    std::map<unsigned, std::vector<uint64_t>> result;
    
    // Query each shard's index for matching series
    std::vector<seastar::future<std::pair<unsigned, std::vector<uint64_t>>>> futures;
    
    unsigned shardCount = seastar::smp::count;
    if (shardCount == 0) shardCount = 1;  // Handle test environment
    
    for (unsigned shardId = 0; shardId < shardCount; ++shardId) {
        auto f = indexSharded->invoke_on(shardId, 
            [request](LevelDBIndex& index) -> seastar::future<std::vector<uint64_t>> {
                std::vector<uint64_t> allSeriesIds;
                
                // Get all series for this measurement, filtered by tags
                if (request.scopes.empty()) {
                    // No tag filters, get all series for the measurement
                    co_await index.findSeries(request.measurement).then(
                        [&allSeriesIds](std::vector<uint64_t> ids) {
                            allSeriesIds = std::move(ids);
                            return seastar::make_ready_future<>();
                        }
                    );
                } else {
                    // Apply tag filters
                    // Start with the most selective tag filter (if we can determine it)
                    // For now, use the first tag filter to get initial set
                    if (!request.scopes.empty()) {
                        auto firstTag = request.scopes.begin();
                        co_await index.findSeriesByTag(request.measurement, 
                                                       firstTag->first, 
                                                       firstTag->second).then(
                            [&allSeriesIds](std::vector<uint64_t> ids) {
                                allSeriesIds = std::move(ids);
                                return seastar::make_ready_future<>();
                            }
                        );
                        
                        // TODO: Further filter by additional tags if needed
                        // This would require fetching metadata for each series
                        // and checking if all tags match
                    }
                }
                
                // Filter by requested fields if specific fields are requested
                if (!request.requestsAllFields()) {
                    std::vector<uint64_t> filteredIds;
                    for (uint64_t seriesId : allSeriesIds) {
                        // Get metadata to check the field
                        auto metadata = co_await index.getSeriesMetadata(seriesId);
                        if (metadata.has_value()) {
                            // Check if the field matches one of the requested fields
                            if (std::find(request.fields.begin(), request.fields.end(), 
                                         metadata->field) != request.fields.end()) {
                                filteredIds.push_back(seriesId);
                            }
                        }
                    }
                    allSeriesIds = std::move(filteredIds);
                }
                
                co_return allSeriesIds;
            }).then([shardId](std::vector<uint64_t> ids) {
                return std::make_pair(shardId, ids);
            });
        
        futures.push_back(std::move(f));
    }
    
    // Wait for all shards to complete
    auto results = co_await seastar::when_all_succeed(futures.begin(), futures.end());
    
    for (const auto& [shardId, seriesIds] : results) {
        if (!seriesIds.empty()) {
            result[shardId] = seriesIds;
        }
    }
    
    co_return result;
}

std::map<unsigned, std::vector<uint64_t>> 
QueryPlanner::findMatchingSeriesIdsSync(
    const QueryRequest& request,
    LevelDBIndex* index) {
    
    std::map<unsigned, std::vector<uint64_t>> result;
    
    // If no index provided, use mock implementation for unit tests
    if (!index) {
        // Simplified version for testing without actual index operations
        if (!request.measurement.empty()) {
            // Simulate finding series based on request
            std::set<std::string> queryFields;
            
            if (request.requestsAllFields()) {
                // Simulate having some default fields
                queryFields = {"value", "field1", "field2"};
            } else {
                queryFields.insert(request.fields.begin(), request.fields.end());
            }
            
            // For each field, simulate a series ID
            uint64_t mockSeriesId = 1000;
            for (const auto& field : queryFields) {
                // Calculate which shard this series belongs to
                unsigned shardId = calculateShardForSeries(
                    request.measurement, request.scopes, field);
                
                result[shardId].push_back(mockSeriesId++);
            }
        }
        return result;
    }
    
    // Use actual index to find series
    // Note: This is a synchronous version, mainly for testing
    // In production, the async version above should be used
    
    // For testing, we'll simulate what would happen on each shard
    // In reality, each shard would only see its own data
    unsigned shardCount = seastar::smp::count;
    if (shardCount == 0) shardCount = 1;
    
    // Simulate querying the index
    // In a real scenario, we'd need to run this on the actual shard
    // For now, we'll just demonstrate the logic
    
    // This would need to be implemented with actual async operations
    // For testing purposes, we'll keep the mock implementation
    
    return result;
}

std::map<unsigned, std::vector<uint64_t>> QueryPlanner::mapSeriesToShards(
    const std::vector<uint64_t>& seriesIds,
    const std::string& measurement,
    const std::map<std::string, std::string>& tags,
    const std::vector<std::string>& fields) {
    
    std::map<unsigned, std::vector<uint64_t>> shardMap;
    
    // For each series ID, determine its shard
    for (uint64_t seriesId : seriesIds) {
        // In a real implementation, we would need to look up the series metadata
        // to determine which field this series ID corresponds to
        // For now, distribute based on series ID hash
        
        unsigned shardCount = seastar::smp::count;
        if (shardCount == 0) shardCount = 1;  // Handle test environment
        
        unsigned shardId = seriesId % shardCount;
        shardMap[shardId].push_back(seriesId);
    }
    
    return shardMap;
}

unsigned QueryPlanner::calculateShardForSeries(
    const std::string& measurement,
    const std::map<std::string, std::string>& tags,
    const std::string& field) {
    
    // Build the series key and hash it to determine shard
    std::string seriesKey = buildSeriesKeyForSharding(measurement, tags, field);
    
    unsigned shardCount = seastar::smp::count;
    if (shardCount == 0) shardCount = 1;  // Handle test environment
    
    size_t hash = std::hash<std::string>{}(seriesKey);
    return hash % shardCount;
}

std::string QueryPlanner::buildSeriesKeyForSharding(
    const std::string& measurement,
    const std::map<std::string, std::string>& tags,
    const std::string& field) {
    
    std::string seriesKey = measurement;
    
    // Add sorted tags for consistent hashing
    for (const auto& [tagKey, tagValue] : tags) {
        seriesKey += "," + tagKey + "=" + tagValue;
    }
    
    // Add field name
    seriesKey += "," + field;
    
    return seriesKey;
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
            (!value.empty() && value[0] == '/')) {
            return true;
        }
    }
    
    return false;
}

} // namespace tsdb