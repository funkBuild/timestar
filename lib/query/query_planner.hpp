#pragma once

#include "native_index.hpp"
#include "query_parser.hpp"
#include "series_id.hpp"
#include "series_matcher.hpp"

#include <map>
#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <set>
#include <vector>

namespace timestar {

// Represents a query to be executed on a specific shard
struct ShardQuery {
    unsigned shardId;
    std::vector<SeriesId128> seriesIds;  // Series IDs to query on this shard
    std::set<std::string> fields;        // Fields to retrieve
    uint64_t startTime;
    uint64_t endTime;
    bool requiresAllSeries;  // If true, query all matching series on shard
};

// Complete query execution plan
struct QueryPlan {
    std::vector<ShardQuery> shardQueries;
    AggregationMethod aggregation;
    uint64_t aggregationInterval = 0;  // Time interval for bucketing (0 = no bucketing)
    std::vector<std::string> groupByTags;
    bool requiresMerging;

    // Metadata for optimization
    size_t estimatedSeriesCount = 0;
    size_t estimatedPointCount = 0;
};

class QueryPlanner {
public:
    // Create execution plan from query request
    // This will use the index to find matching series and map them to shards
    seastar::future<QueryPlan> createPlan(const QueryRequest& request, seastar::sharded<index::NativeIndex>* indexSharded);

    // Synchronous version for testing
    QueryPlan createPlanSync(const QueryRequest& request, index::NativeIndex* index);

    // Public for testing
    bool requiresAllShards(const QueryRequest& request);

    std::string buildSeriesKeyForSharding(const std::string& measurement,
                                          const std::map<std::string, std::string>& tags, const std::string& field);

private:
    // Find all series IDs matching the query filters
    seastar::future<std::vector<std::vector<SeriesId128>>> findMatchingSeriesIds(
        const QueryRequest& request, seastar::sharded<index::NativeIndex>* indexSharded);

    // Synchronous version for testing
    std::vector<std::vector<SeriesId128>> findMatchingSeriesIdsSync(const QueryRequest& request, index::NativeIndex* index);

    // Map series IDs to their respective shards
    std::vector<std::vector<SeriesId128>> mapSeriesToShards(const std::vector<SeriesId128>& seriesIds,
                                                            const std::string& measurement,
                                                            const std::map<std::string, std::string>& tags,
                                                            const std::vector<std::string>& fields);

    // Calculate which shard a series belongs to
    unsigned calculateShardForSeries(const std::string& measurement, const std::map<std::string, std::string>& tags,
                                     const std::string& field);
};

}  // namespace timestar
