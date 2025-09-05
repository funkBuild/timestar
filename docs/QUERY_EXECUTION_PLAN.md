# Query Execution Implementation Plan

## Overview

This document outlines the complete implementation plan for distributed query execution with aggregation and group-by functionality. The system will distribute sub-queries across shards, execute them locally, and reassemble results with proper aggregation.

## Query Execution Flow

```
User Query Request
    ↓
Query Parser (✅ Complete)
    ↓
Query Planner
    ↓
Shard Distribution
    ↓
Parallel Shard Execution
    ↓
Result Collection
    ↓
Aggregation & Group By
    ↓
Response Formatting (✅ Complete)
```

## Phase 1: Query Planning and Distribution

### 1.1 Query Planner Component

Create a query planner that transforms user queries into distributed execution plans:

```cpp
// lib/query_planner.hpp
struct ShardQuery {
    unsigned shardId;
    std::vector<std::string> seriesKeys;  // Series to query on this shard
    QueryRequest request;                  // Original request
    bool requiresAllSeries;                // If true, query all series on shard
};

struct QueryPlan {
    std::vector<ShardQuery> shardQueries;
    AggregationMethod finalAggregation;
    std::vector<std::string> groupByKeys;
    bool requiresMerging;
};

class QueryPlanner {
public:
    // Create execution plan from query request
    QueryPlan createPlan(const QueryRequest& request, LevelDBIndex* index);
    
private:
    // Determine which series match the query filters
    std::vector<std::string> findMatchingSeries(
        const std::string& measurement,
        const std::map<std::string, std::string>& scopes,
        LevelDBIndex* index);
    
    // Map series to shards
    std::map<unsigned, std::vector<std::string>> mapSeriesToShards(
        const std::vector<std::string>& seriesKeys);
};
```

### 1.2 Series Matching

Implement series discovery based on query scopes:

```cpp
// lib/series_matcher.hpp
class SeriesMatcher {
public:
    // Check if series tags match query scopes
    static bool matches(
        const std::map<std::string, std::string>& seriesTags,
        const std::map<std::string, std::string>& queryScopes);
    
    // Support for future wildcard/regex matching
    static bool matchesPattern(
        const std::string& value,
        const std::string& pattern);
};
```

## Phase 2: Shard-Local Query Execution

### 2.1 Engine Query Methods

Extend Engine class with local query execution:

```cpp
// lib/engine.hpp additions
class Engine {
public:
    // Execute query on local shard data only
    seastar::future<std::vector<SeriesResult>> executeLocalQuery(
        const std::vector<std::string>& seriesKeys,
        uint64_t startTime,
        uint64_t endTime,
        const std::vector<std::string>& fields);
    
    // Get all series matching measurement and tags
    seastar::future<std::vector<std::string>> findLocalSeries(
        const std::string& measurement,
        const std::map<std::string, std::string>& tagFilters);
    
private:
    // Query specific series from TSM and memory store
    seastar::future<SeriesResult> querySeriesLocal(
        const std::string& seriesKey,
        uint64_t startTime,
        uint64_t endTime);
};
```

### 2.2 Local Query Implementation

```cpp
// lib/engine.cpp
seastar::future<std::vector<SeriesResult>> 
Engine::executeLocalQuery(
    const std::vector<std::string>& seriesKeys,
    uint64_t startTime,
    uint64_t endTime,
    const std::vector<std::string>& fields) {
    
    std::vector<SeriesResult> results;
    
    for (const auto& seriesKey : seriesKeys) {
        // Parse series key to get measurement and tags
        auto [measurement, tags, field] = parseSeriesKey(seriesKey);
        
        // Query from memory store
        auto memData = co_await memoryStore->query(seriesKey, startTime, endTime);
        
        // Query from TSM files
        auto tsmData = co_await tsmFileManager->query(seriesKey, startTime, endTime);
        
        // Merge and sort by timestamp
        auto merged = mergeTimeSeries(memData, tsmData);
        
        // Build result
        SeriesResult result;
        result.measurement = measurement;
        result.tags = tags;
        result.fields[field] = merged;
        
        results.push_back(result);
    }
    
    co_return results;
}
```

## Phase 3: Query Distribution and Collection

### 3.1 Query Coordinator

Implement the distributed query coordinator:

```cpp
// lib/http_query_handler.cpp
seastar::future<std::vector<SeriesResult>> 
HttpQueryHandler::queryAllShards(const QueryRequest& request) {
    // Create query plan
    QueryPlanner planner;
    auto plan = planner.createPlan(request, &index);
    
    // Execute queries on each shard in parallel
    std::vector<seastar::future<std::vector<SeriesResult>>> futures;
    
    for (const auto& shardQuery : plan.shardQueries) {
        auto f = engineSharded->invoke_on(shardQuery.shardId,
            [sq = shardQuery](Engine& engine) -> seastar::future<std::vector<SeriesResult>> {
                if (sq.requiresAllSeries) {
                    // Query all series matching measurement/tags
                    auto series = co_await engine.findLocalSeries(
                        sq.request.measurement, sq.request.scopes);
                    co_return co_await engine.executeLocalQuery(
                        series, sq.request.startTime, sq.request.endTime, sq.request.fields);
                } else {
                    // Query specific series
                    co_return co_await engine.executeLocalQuery(
                        sq.seriesKeys, sq.request.startTime, sq.request.endTime, sq.request.fields);
                }
            });
        futures.push_back(std::move(f));
    }
    
    // Wait for all shards to complete
    auto shardResults = co_await seastar::when_all_succeed(futures.begin(), futures.end());
    
    // Merge results from all shards
    co_return mergeResults(shardResults);
}
```

### 3.2 Result Merging

Implement intelligent result merging:

```cpp
std::vector<SeriesResult> HttpQueryHandler::mergeResults(
    std::vector<std::vector<SeriesResult>> shardResults) {
    
    // Map to collect results by series key
    std::map<std::string, SeriesResult> mergedMap;
    
    for (const auto& shardResult : shardResults) {
        for (const auto& series : shardResult) {
            std::string key = buildSeriesKey(series.measurement, series.tags);
            
            if (mergedMap.find(key) == mergedMap.end()) {
                mergedMap[key] = series;
            } else {
                // Merge timestamps and values
                for (auto& [field, data] : series.fields) {
                    auto& existing = mergedMap[key].fields[field];
                    
                    // Combine and sort by timestamp
                    existing.first.insert(existing.first.end(), 
                                        data.first.begin(), data.first.end());
                    existing.second.insert(existing.second.end(),
                                         data.second.begin(), data.second.end());
                    
                    // Sort by timestamp
                    sortByTimestamp(existing.first, existing.second);
                    
                    // Remove duplicates
                    removeDuplicateTimestamps(existing.first, existing.second);
                }
            }
        }
    }
    
    // Convert map to vector
    std::vector<SeriesResult> merged;
    for (auto& [key, series] : mergedMap) {
        merged.push_back(std::move(series));
    }
    
    return merged;
}
```

## Phase 4: Aggregation Implementation

### 4.1 Aggregation Functions

Implement the aggregation methods:

```cpp
// lib/aggregator.hpp
class Aggregator {
public:
    // Apply aggregation to time series data
    static std::pair<std::vector<uint64_t>, std::vector<double>>
    aggregate(
        const std::vector<uint64_t>& timestamps,
        const std::vector<double>& values,
        AggregationMethod method,
        std::optional<uint64_t> interval = std::nullopt);
    
private:
    static double calculateAvg(const std::vector<double>& values);
    static double calculateMin(const std::vector<double>& values);
    static double calculateMax(const std::vector<double>& values);
    static double calculateSum(const std::vector<double>& values);
    static std::pair<uint64_t, double> getLatest(
        const std::vector<uint64_t>& timestamps,
        const std::vector<double>& values);
};
```

### 4.2 Apply Aggregation

```cpp
void HttpQueryHandler::applyAggregation(
    std::vector<SeriesResult>& results,
    const QueryRequest& request) {
    
    for (auto& series : results) {
        for (auto& [field, data] : series.fields) {
            // Apply aggregation to each field
            auto aggregated = Aggregator::aggregate(
                data.first,  // timestamps
                data.second, // values
                request.aggregation
            );
            
            data = aggregated;
        }
    }
}
```

## Phase 5: Group By Implementation

### 5.1 Group By Logic

Implement grouping functionality:

```cpp
std::vector<SeriesResult> HttpQueryHandler::applyGroupBy(
    const std::vector<SeriesResult>& results,
    const QueryRequest& request) {
    
    if (!request.hasGroupBy()) {
        return results;
    }
    
    // Group series by specified tags
    std::map<std::string, std::vector<const SeriesResult*>> groups;
    
    for (const auto& series : results) {
        // Build group key from specified tags
        std::string groupKey;
        for (const auto& tagKey : request.groupByTags) {
            auto it = series.tags.find(tagKey);
            if (it != series.tags.end()) {
                if (!groupKey.empty()) groupKey += ",";
                groupKey += tagKey + "=" + it->second;
            }
        }
        
        groups[groupKey].push_back(&series);
    }
    
    // Aggregate within each group
    std::vector<SeriesResult> grouped;
    
    for (const auto& [groupKey, groupSeries] : groups) {
        SeriesResult groupResult;
        groupResult.measurement = groupSeries[0]->measurement;
        
        // Set group tags
        for (const auto& tagKey : request.groupByTags) {
            auto it = groupSeries[0]->tags.find(tagKey);
            if (it != groupSeries[0]->tags.end()) {
                groupResult.tags[tagKey] = it->second;
            }
        }
        
        // Merge and aggregate all series in group
        for (const auto* series : groupSeries) {
            for (const auto& [field, data] : series->fields) {
                if (groupResult.fields.find(field) == groupResult.fields.end()) {
                    groupResult.fields[field] = data;
                } else {
                    // Merge timestamps and values
                    auto& existing = groupResult.fields[field];
                    mergeAndAggregate(existing, data, request.aggregation);
                }
            }
        }
        
        grouped.push_back(groupResult);
    }
    
    return grouped;
}
```

## Phase 6: Optimization and Caching

### 6.1 Query Cache

Implement result caching:

```cpp
// lib/query_cache.hpp
class QueryCache {
private:
    struct CacheEntry {
        QueryRequest request;
        QueryResponse response;
        std::chrono::steady_clock::time_point expiry;
    };
    
    std::unordered_map<std::string, CacheEntry> cache;
    std::mutex cacheMutex;
    
public:
    std::optional<QueryResponse> get(const QueryRequest& request);
    void put(const QueryRequest& request, const QueryResponse& response);
    std::string buildCacheKey(const QueryRequest& request);
};
```

### 6.2 Performance Metrics

Add query performance tracking:

```cpp
struct QueryMetrics {
    std::atomic<uint64_t> totalQueries{0};
    std::atomic<uint64_t> cacheHits{0};
    std::atomic<uint64_t> cacheMisses{0};
    std::atomic<uint64_t> totalPointsScanned{0};
    std::atomic<uint64_t> totalSeriesQueried{0};
    std::chrono::nanoseconds totalExecutionTime{0};
    
    void recordQuery(const QueryResponse& response);
    void recordCacheHit();
    void recordCacheMiss();
};
```

## Implementation Schedule

### Week 1: Core Query Execution
- [ ] Implement QueryPlanner
- [ ] Add Engine::executeLocalQuery
- [ ] Create SeriesMatcher
- [ ] Implement shard distribution logic

### Week 2: Result Processing
- [ ] Implement result merging
- [ ] Add Aggregator class
- [ ] Implement all aggregation functions
- [ ] Add timestamp sorting and deduplication

### Week 3: Group By and Optimization
- [ ] Implement group-by logic
- [ ] Add query cache
- [ ] Implement performance metrics
- [ ] Add query timeout handling

### Week 4: Testing and Polish
- [ ] Write integration tests
- [ ] Add performance benchmarks
- [ ] Create end-to-end test scenarios
- [ ] Document query API

## Testing Strategy

### Unit Tests
- Test each aggregation function
- Test series matching logic
- Test result merging
- Test group-by logic

### Integration Tests
```cpp
TEST(QueryIntegration, DistributedAggregation) {
    // Insert data across multiple shards
    // Query with aggregation
    // Verify results are correctly aggregated
}

TEST(QueryIntegration, GroupByAcrossShards) {
    // Insert data with various tags
    // Query with group by
    // Verify grouping works across shards
}

TEST(QueryIntegration, TimeRangeFiltering) {
    // Insert data with various timestamps
    // Query with specific time range
    // Verify only data in range is returned
}
```

### Performance Tests
- Measure query latency vs data size
- Test concurrent query handling
- Benchmark aggregation performance
- Test cache effectiveness

## Example Query Execution

### Input Query
```json
{
  "query": "avg:temperature(value){location:us-west} by {sensor}",
  "startTime": "01-01-2024 00:00:00",
  "endTime": "01-01-2024 23:59:59"
}
```

### Execution Steps

1. **Query Planning**
   - Find all series matching "temperature" with location="us-west"
   - Map series to shards based on hash(measurement,tags,field)

2. **Shard Distribution**
   - Shard 0: [temp,location=us-west,sensor=s1,value], [temp,location=us-west,sensor=s2,value]
   - Shard 1: [temp,location=us-west,sensor=s3,value]
   - Shard 2: [temp,location=us-west,sensor=s4,value], [temp,location=us-west,sensor=s5,value]
   - Shard 3: [temp,location=us-west,sensor=s6,value]

3. **Local Execution**
   - Each shard queries its local TSM files and memory store
   - Returns raw time series data

4. **Result Collection**
   - Collect results from all shards
   - Merge by series key

5. **Group By Application**
   - Group results by "sensor" tag
   - Results in 6 groups (s1, s2, s3, s4, s5, s6)

6. **Aggregation**
   - Calculate average for each sensor group
   - Return single value per sensor

### Output Response
```json
{
  "status": "success",
  "series": [
    {
      "measurement": "temperature",
      "tags": {"sensor": "s1"},
      "fields": {
        "value": {
          "timestamps": [1704067200000000000],
          "values": [22.5]
        }
      }
    },
    // ... more sensors
  ],
  "statistics": {
    "series_count": 6,
    "point_count": 6,
    "execution_time_ms": 45.2
  }
}
```

## Success Criteria

1. **Correctness**
   - Aggregations produce accurate results
   - Group by works correctly across shards
   - Time filtering is precise

2. **Performance**
   - Sub-100ms latency for simple queries
   - Linear scaling with shard count
   - Effective caching reduces repeated query load

3. **Reliability**
   - Handles shard failures gracefully
   - Timeout handling for slow queries
   - Memory-bounded result sets

4. **Usability**
   - Clear error messages
   - Intuitive query format
   - Good documentation and examples